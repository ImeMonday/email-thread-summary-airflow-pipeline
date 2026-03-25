EPIC: Build Data Pipeline — Local CSV → PostgreSQL via Dockerized Airflow

Dataset
Name: email_thread_summary
Source: Local CSV download (SharePoint → local → Docker mount)
Target table: public.email_thread_summary

Overview

This project implements a production-style Apache Airflow pipeline that ingests a local CSV dataset, validates it against a strict schema contract, applies deterministic transformations, and loads the data into PostgreSQL.

The pipeline is fully containerized using Docker Compose and is safe to re-run without creating duplicate records.

Architecture

DAG ID: email_thread_summary_ingest

Task flow:
file_check
load_contract
validate_schema
transform
create_table
load_to_postgres

Data locations

CSV input inside container
/opt/airflow/projects/email_thread_summary/sample_data/

Schema contract
config/schema_expected.yaml

Table DDL
config/create_table.sql

Schema validation

The pipeline enforces a schema contract defined in schema_expected.yaml.

Validation checks include:
column presence
data types
nullability

If the CSV schema does not match the contract, the DAG fails before any database write occurs. This prevents silent data corruption.

Transform logic

The transform step applies minimal, deterministic cleaning:
strip leading and trailing whitespace
convert empty strings to NULL
no enrichment or derived columns

Idempotency and “skip done” logic

Design decision

This dataset does not include a status field such as done.

Instead of status-based skipping, the pipeline implements idempotency at the database level using:
primary key: thread_id
load strategy: INSERT with ON CONFLICT (thread_id) DO UPDATE

Resulting behavior

Re-running the DAG does not create duplicate records
Existing rows are updated deterministically
The pipeline is safe for retries, reruns, and backfills

This approach replaces skip-done semantics with a stronger, database-enforced idempotency model.

How to run

Start the environment
docker compose up -d

Trigger the DAG
docker exec -it imeobong-monday-project1-airflow-scheduler-1 airflow dags trigger email_thread_summary_ingest

Verification

Row count and uniqueness check

SELECT COUNT(*) AS row_count, COUNT(DISTINCT thread_id) AS distinct_threads
FROM public.email_thread_summary;

Expected result
row_count equals distinct_threads

Duplicate check

SELECT thread_id, COUNT()
FROM public.email_thread_summary
GROUP BY thread_id
HAVING COUNT() > 1;

Expected result
no rows returned

Schema and constraints check

\d+ public.email_thread_summary

Expected result
primary key exists on thread_id

Troubleshooting

Airflow UI not reachable at localhost:8080
Cause: webserver terminated due to low memory
Resolution:
trigger DAGs via CLI
reduce webserver workers
increase Docker Desktop memory allocation

Metadata database error: database system is in recovery mode
Cause: Airflow metadata DB instability on Windows
Resolution:
use a named Docker volume for the metadata DB
recreate the Docker stack

Runbook

Standard execution procedure
start Docker services
trigger the DAG
verify row counts and uniqueness

Schema change procedure
update schema_expected.yaml
update transform logic if required
update create_table.sql
test with sample CSV
verify schema and counts

Project structure

extraction/email_thread_summary/
MANIFEST.md
README.md
config/
schema_expected.yaml
create_table.sql
sample_data/
email_thread_summary.csv
dags/
email_thread_summary_ingest.py
logs/

Summary

Contract-first ingestion with strict validation
Deterministic transforms
Idempotent and safe database writes
Fully reproducible Dockerized Airflow environment
# Email Thread Summary Ingestion Pipeline

This project demonstrates an end-to-end data pipeline built with Apache Airflow and Postgres.

## Features

- File ingestion from CSV
- Schema validation using YAML contract
- Data transformation and cleaning
- Table creation in Postgres
- Idempotent upsert loading

## Tech Stack

- Apache Airflow
- Docker
- PostgreSQL
- Python (pandas, psycopg2)

## Pipeline Flow

1. file_check
2. load_contract
3. validate_schema
4. transform
5. create_table
6. load_to_postgres

## How to Run

```bash
docker compose up -d
