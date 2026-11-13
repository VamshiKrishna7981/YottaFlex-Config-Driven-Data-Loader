import csv
import json
import logging
import os
import sys
import uuid
import errno
import time

from datetime import datetime, date, timedelta
from pathlib import Path

import duckdb


# ==========================================================
# PROJECT PATHS AND LOGGING
# ==========================================================

BASE_DIR = Path(__file__).resolve().parent.parent

CONFIG_PATH = BASE_DIR / "config" / "pipeline.json"

LOG_DIR = BASE_DIR / "outputs" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "pipeline.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ==========================================================
# CONFIGURATION
# ==========================================================

def load_config():

    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {CONFIG_PATH}"
        )

    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        config = json.load(file)

    logger.info(
        "Configuration loaded successfully"
    )

    return config


def resolve_path(path):

    path = Path(path)

    if path.is_absolute():
        return path

    return BASE_DIR / path


def quote_identifier(name):

    return '"' + str(name).replace('"', '""') + '"'


def get_schema(config):

    schema = config["schema"]

    if isinstance(schema, dict) and "columns" in schema:
        return schema["columns"]

    if isinstance(schema, list):

        return {
            item["name"]: item
            for item in schema
        }

    return schema


def get_keys(config):

    keys_config = config.get("keys", {})

    if isinstance(keys_config, dict):

        keys = keys_config.get(
            "primary_key", []
        )

    else:

        keys = keys_config

    if not keys:

        keys = config.get(
            "primary_key",
            ["order_id"]
        )

    if isinstance(keys, str):
        keys = [keys]

    if not keys:
        raise ValueError(
            "At least one primary key is required."
        )

    return keys


def get_column_type(column_config):

    if isinstance(column_config, str):
        return column_config.upper()

    return column_config.get(
        "type", "VARCHAR"
    ).upper()


def is_nullable(column_config):

    if isinstance(column_config, str):
        return True

    return column_config.get(
        "nullable", True
    )


# ==========================================================
# SOURCE INGESTION
# ==========================================================

def read_source(config):

    source = config["source"]

    source_path = resolve_path(source["path"])

    if not source_path.exists():
        raise FileNotFoundError(
            f"Source file not found: {source_path}"
        )

    file_format = source.get("format", "csv").lower()

    headers = []
    rows = []

    # ==========================================
    # CSV SOURCE
    # ==========================================

    if file_format == "csv":

        with open(
            source_path,
            "r",
            encoding=source.get("encoding", "utf-8"),
            newline=""
        ) as file:

            reader = csv.DictReader(
                file,
                delimiter=source.get("delimiter", ","),
                skipinitialspace=True
            )

            if not reader.fieldnames:
                raise ValueError(
                    "CSV file is empty or has no header."
                )

            headers = [
                header.strip()
                for header in reader.fieldnames
            ]

            if len(headers) != len(set(headers)):
                raise ValueError(
                    "CSV contains duplicate column names."
                )

            for row in reader:

                cleaned_row = {
                    key.strip(): (
                        value.strip()
                        if value is not None
                        else ""
                    )
                    for key, value in row.items()
                    if key is not None
                }

                rows.append(cleaned_row)

    # ==========================================
    # JSON SOURCE
    # ==========================================

        # ==========================================
    # JSON SOURCE
    # ==========================================

    elif file_format == "json":

        with open(
            source_path,
            "r",
            encoding=source.get("encoding", "utf-8")
        ) as file:

            json_data = json.load(file)

        if isinstance(json_data, dict):

            if isinstance(json_data.get("records"), list):
                json_data = json_data["records"]
            else:
                json_data = [json_data]

        if not isinstance(json_data, list):
            raise ValueError(
                "JSON must contain an array of objects."
            )

        if json_data:

            if not all(isinstance(row, dict) for row in json_data):
                raise ValueError(
                    "Every JSON record must be an object."
                )

            headers = [
                str(key).strip()
                for key in json_data[0].keys()
            ]

            if len(headers) != len(set(headers)):
                raise ValueError(
                    "JSON contains duplicate column names after trimming."
                )

            for row in json_data:

                cleaned_row = {}

                for key, value in row.items():

                    cleaned_key = str(key).strip()

                    if cleaned_key in cleaned_row:
                        raise ValueError(
                            f"Duplicate JSON column: {cleaned_key}"
                        )

                    cleaned_row[cleaned_key] = (
                        value.strip()
                        if isinstance(value, str)
                        else value
                    )

                rows.append(cleaned_row)

    # ==========================================
    # PARQUET SOURCE
    # ==========================================
    elif file_format == "parquet":

        connection = duckdb.connect(database=":memory:")

        try:
            result = connection.execute(
                "SELECT * FROM read_parquet(?)",
                [str(source_path)]
            )

            raw_headers = [
                column[0]
                for column in result.description
            ]

            headers = [
                str(header).strip()
                for header in raw_headers
            ]

            if len(headers) != len(set(headers)):
                raise ValueError(
                    "Parquet contains duplicate column names."
                )

            records = result.fetchall()

            for record in records:
                cleaned_row = {}

                for key, value in zip(raw_headers, record):
                    cleaned_value = (
                        value.strip()
                        if isinstance(value, str)
                        else value
                    )

                    cleaned_row[str(key).strip()] = cleaned_value

                rows.append(cleaned_row)

        finally:
            connection.close()
    # ==========================================
    # UNSUPPORTED FORMAT
    # ==========================================

    else:

        raise ValueError(
            f"Unsupported source format: {file_format}. "
            "Supported formats: csv, json, parquet."
        )

    logger.info(
        "Source format: %s",
        file_format
    )

    logger.info(
        "Source records read: %s",
        len(rows)
    )

    return headers, rows
# ==========================================================
# SOURCE READING WITH RETRY
# ==========================================================

def read_source_with_retry(config):

    retry_config = config.get("retry", {})

    enabled = retry_config.get("enabled", True)
    max_attempts = int(retry_config.get("max_attempts", 3))
    delay_seconds = float(
        retry_config.get("delay_seconds", 2)
    )

    if max_attempts < 1 or delay_seconds < 0:
        raise ValueError("Invalid retry configuration.")

    if not enabled:
        return read_source(config)

    for attempt in range(1, max_attempts + 1):

        try:
            return read_source(config)

        except OSError as error:

            transient_errors = {
                errno.EAGAIN,
                errno.EINTR,
                errno.EBUSY
            }

            windows_error = getattr(error, "winerror", None)

            is_transient = (
                error.errno in transient_errors
                or windows_error in (32, 33)
            )

            if not is_transient or attempt == max_attempts:
                raise

            logger.warning(
                "Temporary source read failure. "
                "Attempt %s/%s. Retrying in %s seconds. Error: %s",
                attempt,
                max_attempts,
                delay_seconds,
                error
            )

            time.sleep(delay_seconds)

    raise RuntimeError("Source reading failed after retries.")    

# ==========================================================
# DATA TYPE CONVERSION
# ==========================================================

def convert_value(value, data_type):

    if value is None or value == "":
        return None

    data_type = data_type.upper()

    if data_type in (
        "VARCHAR",
        "STRING",
        "TEXT"
    ):

        return str(value)

    if data_type in (
        "INTEGER",
        "INT",
        "BIGINT"
    ):

        return int(value)

    if data_type in (
        "DOUBLE",
        "FLOAT",
        "DECIMAL"
    ):

        return float(value)

    if data_type in (
        "BOOLEAN",
        "BOOL"
    ):

        normalized = str(value).lower()

        if normalized in ("true", "1", "yes"):
            return True

        if normalized in ("false", "0", "no"):
            return False

        raise ValueError(
            f"Invalid BOOLEAN value: {value}"
        )

    if data_type in (
        "TIMESTAMP",
        "DATETIME"
    ):

        return datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        ).replace(tzinfo=None)

    if data_type == "DATE":

        return date.fromisoformat(
            str(value)
        )

    raise ValueError(
        f"Unsupported data type: {data_type}"
    )


# ==========================================================
# CONFIGURATION-DRIVEN VALIDATION
# ==========================================================

def validate_data(headers, rows, config):

    schema = get_schema(config)

    validation_config = config.get(
        "validation", {}
    )

    expected_columns = set(schema.keys())
    actual_columns = set(headers)

    missing_columns = (
        expected_columns - actual_columns
    )

    unexpected_columns = (
        actual_columns - expected_columns
    )

    if missing_columns or unexpected_columns:

        raise ValueError(
            f"Schema mismatch. "
            f"Missing: {missing_columns}; "
            f"Unexpected: {unexpected_columns}"
        )

    required_fields = set(
        validation_config.get(
            "required_fields", []
        )
    )

    allowed_values = validation_config.get(
        "allowed_values", {}
    )

    rules = validation_config.get(
        "rules", {}
    )

    keys = get_keys(config)

    valid_rows = [] 
    rejected_rows = []

    seen_keys = set()

    for row_number, row in enumerate(
        rows,
        start=2
    ):

        try:

            converted_row = {}

            # ------------------------------------------
            # TYPE CONVERSION AND NULL VALIDATION
            # ------------------------------------------

            for column, column_config in schema.items():

                value = row.get(column)

                if (
                    value is None
                    or (
                        isinstance(value, str)
                        and value.strip() == ""
                    )
                ):

                    if (
                        not is_nullable(column_config)
                        or column in required_fields
                    ):

                        raise ValueError(
                            f"{column} cannot be NULL"
                        )

                converted_row[column] = convert_value(
                    value,
                    get_column_type(column_config)
                )

            # ------------------------------------------
            # PRIMARY KEY VALIDATION
            # ------------------------------------------

            key_values = tuple(
                converted_row.get(key)
                for key in keys
            )

            if any(
                value is None
                for value in key_values
            ):

                raise ValueError(
                    "Primary key cannot be NULL"
                )

            if key_values in seen_keys:

                raise ValueError(
                    f"Duplicate primary key: {key_values}"
                )

            # Do not reserve this key yet. The record must
            # pass every validation rule before it counts
            # as a valid occurrence of the primary key.

            # ------------------------------------------
            # ALLOWED VALUES VALIDATION
            # ------------------------------------------

            for column, allowed_list in allowed_values.items():

                value = converted_row.get(column)

                if value is not None:

                    if value not in allowed_list:

                        raise ValueError(
                            f"Invalid value '{value}' "
                            f"for column '{column}'. "
                            f"Allowed values: {allowed_list}"
                        )

            # ------------------------------------------
            # MINIMUM AMOUNT VALIDATION
            # ------------------------------------------

            if "amount_minimum" in rules:

                amount = converted_row.get("amount")

                minimum = rules["amount_minimum"]

                if (
                    amount is not None
                    and amount < minimum
                ):

                    raise ValueError(
                        f"Amount must be at least {minimum}"
                    )

            seen_keys.add(key_values)

            valid_rows.append(
                converted_row
            )

        except Exception as error:

            rejected_rows.append({

                "row_number": row_number,

                "reason": str(error),

                "data": json.dumps(
                    row,
                    default=str
                )

            })

    logger.info(
        "Validation completed. Valid: %s | Rejected: %s",
        len(valid_rows),
        len(rejected_rows)
    )

    return valid_rows, rejected_rows


# ==========================================================
# QUARANTINE REJECTED RECORDS
# ==========================================================

def save_rejected_records(rejected_rows, config, run_id):

    quarantine_config = config.get(
        "quarantine", {}
    )

    if not quarantine_config.get(
        "enabled", True
    ):

        logger.info(
            "Quarantine is disabled."
        )

        return

    if not rejected_rows:
        return

    quarantine_path = resolve_path(
        quarantine_config.get(
            "path",
            "data/rejected_orders.csv"
        )
    )

    quarantine_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    fieldnames = [
        "run_id",
        "rejected_at",
        "row_number",
        "reason",
        "data"
    ]

    file_exists = (
        quarantine_path.exists()
        and quarantine_path.stat().st_size > 0
    )

    # Migrate an older quarantine CSV while preserving
    # all historical rows and their original information.
    if file_exists:
        with open(
            quarantine_path,
            "r",
            encoding="utf-8",
            newline=""
        ) as existing_file:
            reader = csv.DictReader(existing_file)
            existing_fields = reader.fieldnames or []
            existing_rows = list(reader)

        if existing_fields != fieldnames:
            temporary_path = quarantine_path.with_suffix(
                quarantine_path.suffix + ".tmp"
            )

            with open(
                temporary_path,
                "w",
                encoding="utf-8",
                newline=""
            ) as temporary_file:
                writer = csv.DictWriter(
                    temporary_file,
                    fieldnames=fieldnames
                )
                writer.writeheader()

                for old_row in existing_rows:
                    migrated_row = {
                        field: old_row.get(field, "")
                        for field in fieldnames
                    }
                    writer.writerow(migrated_row)

            os.replace(
                temporary_path,
                quarantine_path
            )

    rejected_at = datetime.now().isoformat(
        timespec="seconds"
    )

    records_to_write = []

    for rejected_row in rejected_rows:
        records_to_write.append({
            "run_id": run_id,
            "rejected_at": rejected_at,
            "row_number": rejected_row.get("row_number", ""),
            "reason": rejected_row.get("reason", ""),
            "data": rejected_row.get("data", "")
        })

    with open(
        quarantine_path,
        "a",
        encoding="utf-8",
        newline=""
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )

        if not file_exists:
            writer.writeheader()

        writer.writerows(
            records_to_write
        )

    logger.info(
        "Rejected records saved: %s | Count: %s | Run ID: %s",
        quarantine_path,
        len(records_to_write),
        run_id
    )


# ==========================================================
# LATE ARRIVING DATA DETECTION
# ==========================================================


def check_late_records(rows, config):

    late_config = config.get(
        "late_data", {}
    )

    date_column = late_config.get(
        "event_time_column",
        late_config.get(
            "date_column",
            "order_date"
        )
    )

    lookback_days = late_config.get(
        "allowed_lateness_days",
        late_config.get(
            "lookback_days",
            7
        )
    )

    cutoff_date = datetime.now() - timedelta(
        days=lookback_days
    )

    late_count = 0

    for row in rows:

        record_date = row.get(
            date_column
        )

        if record_date is None:
            continue

        if isinstance(
            record_date,
            date
        ) and not isinstance(
            record_date,
            datetime
        ):

            record_date = datetime.combine(
                record_date,
                datetime.min.time()
            )

        if isinstance(record_date, str):

            record_date = datetime.fromisoformat(
                record_date.replace("Z", "+00:00")
            ).replace(tzinfo=None)

        if isinstance(record_date, datetime):

            if record_date < cutoff_date:
                late_count += 1

    logger.info(
        "Late arriving records: %s",
        late_count
    )

    return late_count


# ==========================================================
# DATABASE AND TARGET TABLE
# ==========================================================

def create_target_table(connection, config):

    schema = get_schema(config)

    target = config["target"]

    table_name = target.get(
        "table",
        "orders"
    )

    keys = get_keys(config)

    column_definitions = []

    for column, column_config in schema.items():

        column_type = get_column_type(
            column_config
        )

        nullable = is_nullable(
            column_config
        )

        definition = (
            f"{quote_identifier(column)} {column_type}"
        )

        if not nullable:

            definition += " NOT NULL"

        column_definitions.append(
            definition
        )

    for key in keys:

        if key not in schema:

            raise ValueError(
                f"Primary key column '{key}' "
                f"is missing from schema."
            )

    primary_key = ", ".join(
        quote_identifier(key)
        for key in keys
    )

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS
        {quote_identifier(table_name)}
        (
            {", ".join(column_definitions)},
            PRIMARY KEY ({primary_key})
        )
    """

    connection.execute(
        create_sql
    )

    logger.info(
        "Target table ready: %s",
        table_name
    )

    return table_name


# ==========================================================
# PIPELINE RUN LOGGING
# ==========================================================

def create_run_log_table(connection, config):

    logging_config = config.get(
        "logging", {}
    )

    if not logging_config.get(
        "enabled", True
    ):

        return

    log_table = logging_config.get(
        "table",
        "pipeline_runs"
    )

    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS
        {quote_identifier(log_table)}
        (
            run_id VARCHAR,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            source_records INTEGER,
            valid_records INTEGER,
            rejected_records INTEGER,
            late_records INTEGER,
            status VARCHAR,
            error_message VARCHAR
        )
        """
    )


def save_run_log(
    connection,
    config,
    run_id,
    started_at,
    finished_at,
    source_count,
    valid_count,
    rejected_count,
    late_count,
    status,
    error_message
):

    logging_config = config.get(
        "logging", {}
    )

    if not logging_config.get(
        "enabled", True
    ):

        return

    log_table = logging_config.get(
        "table",
        "pipeline_runs"
    )

    connection.execute(
        f"""
        INSERT INTO {quote_identifier(log_table)}
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            started_at,
            finished_at,
            source_count,
            valid_count,
            rejected_count,
            late_count,
            status,
            error_message
        ]
    )


# ==========================================================
# LOAD DATA INTO TARGET
# ==========================================================

def publish_data(
    connection,
    table_name,
    rows,
    config
):

    load_config = config.get(
        "load", {}
    )

    if isinstance(load_config, str):

        load_mode = load_config.lower()

    else:

        load_mode = load_config.get(
            "mode",
            "upsert"
        ).lower()

    if load_mode not in (
        "append",
        "upsert",
        "full_refresh"
    ):

        raise ValueError(
            f"Unsupported load mode: {load_mode}"
        )

    schema = get_schema(config)

    columns = list(
        schema.keys()
    )

    keys = get_keys(config)

    placeholders = ", ".join(
        ["?" for _ in columns]
    )

    column_names = ", ".join(
        quote_identifier(column)
        for column in columns
    )

    insert_sql = f"""
        INSERT INTO {quote_identifier(table_name)}
        ({column_names})
        VALUES ({placeholders})
    """

    inserted_count = 0

    connection.execute(
        "BEGIN TRANSACTION"
    )

    try:

        # ------------------------------------------
        # FULL REFRESH
        # ------------------------------------------

        if load_mode == "full_refresh":

            connection.execute(
                f"DELETE FROM {quote_identifier(table_name)}"
            )

        # ------------------------------------------
        # APPEND / UPSERT
        # ------------------------------------------

        for row in rows:

            if load_mode == "upsert":

                where_clause = " AND ".join(
                    f"{quote_identifier(key)} = ?"
                    for key in keys
                )

                key_values = [
                    row[key]
                    for key in keys
                ]

                connection.execute(
                    f"""
                    DELETE FROM {quote_identifier(table_name)}
                    WHERE {where_clause}
                    """,
                    key_values
                )

            values = [
                row[column]
                for column in columns
            ]

            connection.execute(
                insert_sql,
                values
            )

            inserted_count += 1

        connection.execute(
            "COMMIT"
        )

        logger.info(
            "Records published: %s | Load mode: %s",
            inserted_count,
            load_mode
        )

        return inserted_count

    except Exception:

        connection.execute(
            "ROLLBACK"
        )

        logger.exception(
            "Data loading failed."
        )

        raise

def connect_database_with_retry(database_path, config):
    retry_config = config.get("database_retry", {})

    enabled = retry_config.get("enabled", True)
    max_attempts = int(retry_config.get("max_attempts", 3))
    delay_seconds = float(
        retry_config.get("delay_seconds", 2)
    )

    if max_attempts < 1 or delay_seconds < 0:
        raise ValueError("Invalid database retry configuration.")

    if not enabled:
        return duckdb.connect(str(database_path))

    for attempt in range(1, max_attempts + 1):
        try:
            connection = duckdb.connect(
                str(database_path)
            )

            logger.info(
                "Database connected successfully on attempt %s.",
                attempt
            )

            return connection

        except (duckdb.IOException, OSError):
            logger.exception(
                "Database connection attempt %s/%s failed.",
                attempt,
                max_attempts
            )

            if attempt == max_attempts:
                raise

            time.sleep(delay_seconds)

    raise RuntimeError("Database connection failed.")    
# ==========================================================
# MAIN PIPELINE EXECUTION
# ==========================================================

def run_pipeline():

    run_id = str(
        uuid.uuid4()
    )

    started_at = datetime.now()

    connection = None

    config = None

    source_count = 0
    valid_count = 0
    rejected_count = 0
    late_count = 0
    inserted_count = 0

    status = "FAILED"

    error_message = None

    try:

        # ------------------------------------------
        # LOAD CONFIGURATION
        # ------------------------------------------

        config = load_config()

        target = config["target"]

        database_path = resolve_path(
            target.get(
                "database",
                target.get(
                    "path",
                    "data/warehouse.duckdb"
                )
            )
        )

        database_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        # ------------------------------------------
        # CONNECT DATABASE
        # ------------------------------------------

        connection = connect_database_with_retry(
            database_path,
            config
        )

        logger.info(
            "Connected to database: %s",
            database_path
        )

        # ------------------------------------------
        # CREATE LOG TABLE
        # ------------------------------------------

        create_run_log_table(
            connection,
            config
        )

        # ------------------------------------------
        # CREATE TARGET TABLE
        # ------------------------------------------

        table_name = create_target_table(
            connection,
            config
        )

        # ------------------------------------------
        # READ SOURCE DATA
        # ------------------------------------------

        headers, rows = read_source_with_retry(
            config
        )

        source_count = len(rows)

        # ------------------------------------------
        # VALIDATE DATA
        # ------------------------------------------

        valid_rows, rejected_rows = validate_data(
            headers,
            rows,
            config
        )

        valid_count = len(
            valid_rows
        )

        rejected_count = len(
            rejected_rows
        )

        # ------------------------------------------
        # QUARANTINE REJECTED RECORDS
        # ------------------------------------------

        save_rejected_records(
            rejected_rows,
            config,
            run_id
        )

        # ------------------------------------------
        # DETECT LATE RECORDS
        # ------------------------------------------

        late_count = check_late_records(
            valid_rows,
            config
        )

        # ------------------------------------------
        # PUBLISH DATA
        # ------------------------------------------

        inserted_count = publish_data(
            connection,
            table_name,
            valid_rows,
            config
        )

        status = "SUCCESS"

        logger.info(
            "Pipeline completed successfully."
        )

    except Exception as error:

        error_message = str(error)

        logger.exception(
            "Pipeline execution failed."
        )

    finally:

        finished_at = datetime.now()

        # ------------------------------------------
        # SAVE EXECUTION LOG
        # ------------------------------------------

        if connection is not None:

            try:

                if config is not None:

                    save_run_log(
                        connection,
                        config,
                        run_id,
                        started_at,
                        finished_at,
                        source_count,
                        valid_count,
                        rejected_count,
                        late_count,
                        status,
                        error_message
                    )

            except Exception as log_error:

                logger.exception(
                    "Run logging failed: %s",
                    log_error
                )

                if status == "SUCCESS":

                    status = "FAILED"

                    error_message = (
                        f"Run logging failed: {log_error}"
                    )

            finally:

                connection.close()

        # ------------------------------------------
        # PIPELINE SUMMARY
        # ------------------------------------------

        print("\n========== PIPELINE SUMMARY ==========")

        print(
            f"Pipeline: "
            f"{config.get('pipeline', {}).get('name', 'Unknown') if config else 'Unknown'}"
        )

        print(
            f"Run ID: {run_id}"
        )

        print(
            f"Source Records: {source_count}"
        )

        print(
            f"Valid Records: {valid_count}"
        )

        print(
            f"Rejected Records: {rejected_count}"
        )

        print(
            f"Late Records: {late_count}"
        )

        print(
            f"Inserted Records: {inserted_count}"
        )

        print(
            f"Status: {status}"
        )

        if error_message:

            print(
                f"Error: {error_message}"
            )

        print(
            "======================================"
        )

    if status != "SUCCESS":

        sys.exit(1)


# ==========================================================
# APPLICATION ENTRY POINT
# ==========================================================

if __name__ == "__main__":

    run_pipeline()
