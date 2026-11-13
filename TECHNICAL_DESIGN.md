# Technical Design Document
## Configuration-Driven Data Loader

## 1. Project Overview

This project implements a configuration-driven data ingestion pipeline using Python and DuckDB.

The primary objective is to separate pipeline configuration from application logic. Source paths, formats, schemas, primary keys, target tables, validation rules and loading strategies are defined through JSON metadata.

The pipeline can therefore be adapted to different datasets without changing its core ingestion logic.

## 2. Architecture

The pipeline follows these major stages:

1. Configuration loading
2. Source data ingestion
3. Data type conversion
4. Schema and business validation
5. Rejected-record quarantine
6. Late-data detection
7. Database table preparation
8. Data publishing
9. Execution logging and summary

### Data Flow

Source Files
    |
    v
Configuration Loader
    |
    v
Source Reader
    |
    v
Data Type Conversion
    |
    v
Validation Engine
    |
    +---- Invalid Records ----> Quarantine CSV
    |
    v
Late Data Detection
    |
    v
DuckDB Target
    |
    v
Execution Summary and Logs

## 3. Technology Choices

### Python
Selected because of its support for data processing, file handling, validation and database integration.

### DuckDB
Selected as an embedded analytical database that supports SQL operations and efficient local data processing.

### JSON
Used as the metadata configuration format because it is readable and easy to modify.

### Supported Input Formats
- CSV
- JSON
- Parquet

## 4. Configuration-Driven Design

The main configuration file is:

config/pipeline.json

It defines:

- Pipeline name and description
- Source file path and format
- Schema and column data types
- Primary keys
- Target database and table
- Load mode
- Required fields
- Allowed values
- Business validation rules
- Quarantine settings
- Late-data settings
- Database retry settings
- Logging settings

This approach reduces hard-coded pipeline-specific behaviour.

## 5. Loading Strategies

Three loading strategies are supported.

### APPEND
Adds incoming valid records to the target table without replacing existing records.

### UPSERT
Uses configured primary keys to identify existing records. Existing matching records are replaced with incoming values, while new records are inserted.

### FULL_REFRESH
Deletes existing target-table records and loads the incoming valid records.

Database publishing uses transactions so that failed publishing operations can be rolled back.

## 6. Data Validation

The validation process includes:

- Required-field validation
- Nullability checks
- Data type conversion
- Configurable allowed-value checks
- Minimum amount validation
- Primary-key duplicate detection

Invalid records are excluded from normal publishing and can be written to the configured quarantine CSV file with rejection reasons.

## 7. Late-Arriving Data

The configuration defines an event-time column, allowed lateness period and policy.

The implementation identifies records outside the configured lateness window and includes their count in the execution summary.

## 8. Error Handling and Reliability

The implementation includes:

- Source reading retry handling for selected transient operating-system errors.
- Configurable database connection retries.
- Logging of pipeline failures.
- Database transaction rollback when publishing fails.
- Run identifiers for execution tracking.
- Rejected-record timestamps and reasons.

Retry behaviour is configurable through the JSON configuration.

## 9. Execution Logging

Pipeline execution details are stored in a DuckDB logging table.

The recorded information includes:

- Run ID
- Start and finish timestamps
- Source record count
- Valid record count
- Rejected record count
- Late record count
- Execution status
- Error message

Application logs are written to the configured log directory.

## 10. Testing and Validation

The following checks were performed manually during development:

| Test | Result |
|---|---|
| CSV ingestion | Executed successfully |
| JSON ingestion | Executed successfully |
| Parquet ingestion | Executed successfully |
| Required-field validation | Tested |
| Configurable minimum amount rule | Tested |
| Duplicate primary-key handling | Tested |
| APPEND mode | Tested on a separate table |
| UPSERT mode | Tested |
| UPSERT idempotency | Tested |
| FULL_REFRESH mode | Tested on a separate table |
| Source retry simulation | Passed |
| Database retry simulation | Passed |
| Independent extracted-project execution | Passed |

### Independent Execution Result

The packaged project was extracted into a separate directory and executed successfully.

Observed result:

- Source records: 7
- Valid records: 6
- Rejected records: 1
- Late records: 1
- Inserted records: 6
- Status: SUCCESS

These tests were performed manually. The included test_pipeline.py file does not currently contain an automated test suite.

## 11. Design Trade-offs

### Configuration Flexibility
A JSON configuration makes the pipeline reusable, but configuration errors must be detected and reported clearly.

### Embedded Database
DuckDB simplifies local deployment and removes the need for a separately managed database server. It is not a replacement for every distributed production database architecture.

### Retry Strategy
Retries help with selected temporary connection and file-access failures. They do not guarantee recovery from every failure.

### UPSERT Implementation
The current implementation uses primary-key-based delete and insert operations inside a transaction. This is straightforward but may need optimization for very large datasets.

## 12. Current Limitations and Future Improvements

Potential improvements include:

- Automated unit and integration tests.
- Performance benchmarking with datasets containing 100,000 or more records.
- More comprehensive recovery and restart handling.
- Referential-integrity validation.
- Additional transformation rules.
- More detailed reconciliation reporting.
- Improved handling of late-arriving records.
- Production monitoring and metrics.
- Optimization of large-scale UPSERT operations.

These are identified as future improvements and are not claimed as completed features.

## 13. Conclusion

The project demonstrates a reusable configuration-driven ingestion pipeline with multiple input formats, configurable validation, multiple loading strategies, rejected-record handling, retry mechanisms and execution logging.

The implementation was executed and independently validated using the packaged project.
