from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
import yaml
import psycopg2
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_DIR = "/opt/airflow/projects/email_thread_summary"
CSV_PATH = f"{PROJECT_DIR}/sample_data/email_thread_summaries.csv"
CONTRACT_PATH = f"{PROJECT_DIR}/config/schema_expected.yaml"
DDL_PATH = f"{PROJECT_DIR}/config/create_table.sql"


def _get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "dev-target-db"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "devdb"),
        user=os.getenv("PG_USER", "dev"),
        password=os.getenv("PG_PASSWORD", "dev"),
    )



def file_check():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found at {CSV_PATH}")



def load_contract(ti):
    if not os.path.exists(CONTRACT_PATH):
        raise FileNotFoundError(f"schema_expected.yaml not found at {CONTRACT_PATH}")

    with open(CONTRACT_PATH, "r", encoding="utf-8") as f:
        contract = yaml.safe_load(f)

    ti.xcom_push(key="contract", value=contract)


def _basic_type_check(series: pd.Series, expected: str, col: str):
    exp = (expected or "").lower().strip()

    if exp in ("bigint", "int", "integer"):
        coerced = pd.to_numeric(series, errors="coerce")
        bad = series.notna() & coerced.isna()
        if bad.any():
            raise ValueError(f"type check failed for {col}")

    if exp in ("text", "varchar", "string"):
        return


def validate_schema(ti):
    contract = ti.xcom_pull(key="contract", task_ids="load_contract")

    cols = contract["columns"]
    expected_cols = [c["name"] for c in cols]

    nullable_map = {c["name"]: c.get("nullable", True) for c in cols}
    type_map = {c["name"]: c.get("type") for c in cols}

    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=False)


    if set(df.columns) != set(expected_cols):
        raise ValueError("Schema mismatch")

    df = df[expected_cols]

    for c in expected_cols:
        df[c] = df[c].astype(str).str.strip()
        df.loc[df[c] == "", c] = pd.NA

    for c, nullable in nullable_map.items():
        if not nullable and df[c].isna().any():
            raise ValueError(f"Null values found in {c}")

    for c, t in type_map.items():
        _basic_type_check(df[c], t, c)

    ti.xcom_push(key="validated_rows", value=df.to_dict(orient="records"))


def transform(ti):
    rows = ti.xcom_pull(key="validated_rows", task_ids="validate_schema")
    df = pd.DataFrame(rows)

    for c in df.columns:
        df[c] = df[c].astype("string").str.strip()
        df.loc[df[c] == "", c] = pd.NA

    ti.xcom_push(key="clean_rows", value=df.to_dict(orient="records"))


def create_table():
    if not os.path.exists(DDL_PATH):
        raise FileNotFoundError(f"{DDL_PATH} not found")

    with open(DDL_PATH, "r", encoding="utf-8") as f:
        ddl = f.read()

    conn = _get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def load_to_postgres(ti):
    contract = ti.xcom_pull(key="contract", task_ids="load_contract")
    rows = ti.xcom_pull(key="clean_rows", task_ids="transform")

    table = contract["table"]
    cols = [c["name"] for c in contract["columns"]]
    pk = contract["primary_key"]

    df = pd.DataFrame(rows)


    for c in contract["columns"]:
        if c["type"].lower() in ("bigint", "int", "integer"):
            df[c["name"]] = pd.to_numeric(df[c["name"]])

    if df.empty:
        return

    insert_cols = ", ".join(cols)
    conflict_cols = ", ".join(pk)
    update_set = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c not in pk])

    sql = f"""
        INSERT INTO {table} ({insert_cols})
        VALUES %s
        ON CONFLICT ({conflict_cols})
        DO UPDATE SET {update_set}
    """

    data = [tuple(row) for row in df.to_numpy()]

    conn = _get_pg_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, data)
        conn.commit()
    finally:
        conn.close()


with DAG(
    dag_id="email_thread_summary_ingest",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["intern-project", "dhap-34"],
) as dag:

    t1 = PythonOperator(task_id="file_check", python_callable=file_check)
    t2 = PythonOperator(task_id="load_contract", python_callable=load_contract)
    t3 = PythonOperator(task_id="validate_schema", python_callable=validate_schema)
    t4 = PythonOperator(task_id="transform", python_callable=transform)
    t5 = PythonOperator(task_id="create_table", python_callable=create_table)
    t6 = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6