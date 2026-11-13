# Configuration-Driven Data Loader

## Overview
A metadata-driven data ingestion pipeline built using Python and DuckDB. Pipeline behaviour is controlled through a JSON configuration file.

## Features
- CSV, JSON and Parquet ingestion.
- Configuration-driven source, schema and target.
- Configurable primary keys.
- APPEND, UPSERT and FULL_REFRESH modes.
- Required-field and business-rule validation.
- Rejected-record quarantine.
- Late-arriving data detection.
- Database connection retries.
- Pipeline execution logging.

## Technology Stack
- Python
- DuckDB
- JSON
- CSV
- Parquet

## Installation
Install dependencies:

    pip install -r requirements.txt

## Configuration
Edit config/pipeline.json to configure the source, schema, primary keys, target database, load mode, validation, quarantine, retry and logging settings.

## Run
From the project root:

    python src/main.py

## Supported Load Modes
- APPEND: Adds incoming records.
- UPSERT: Inserts or updates records using configured primary keys.
- FULL_REFRESH: Replaces existing target-table data.

## Output
The pipeline displays source, valid, rejected, late and inserted record counts along with execution status.

Rejected records are saved to the configured quarantine CSV file.

Logs are stored in outputs/logs/.

## Additional Documentation

- [AI Usage Disclosure and Prompt History](AI_USAGE_DISCLOSURE.md)
- [Technical Design Document](TECHNICAL_DESIGN.md)

## GitHub Repository

https://github.com/VamshiKrishna7981/YottaFlex-Config-Driven-Data-Loader
