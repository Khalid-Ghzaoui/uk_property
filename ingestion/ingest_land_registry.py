# ingestion/ingest_land_registry.py

import pandas as pd
import requests
import io
from google.cloud import bigquery
from datetime import datetime, date
import os
import sys

# ─── CONFIG ───────────────────────────────────────────────
PROJECT_ID = "uk-property-analytics"
DATASET_ID = "property_raw"
TABLE_ID = "ppd_all_transactions"

COLUMNS = [
    "transaction_id", "price", "date_of_transfer_raw",
    "postcode", "property_type", "old_new", "duration",
    "paon", "saon", "street", "locality", "town_city",
    "district", "county", "ppd_category_type", "record_status"
]

SCHEMA = [
    bigquery.SchemaField("transaction_id",       "STRING"),
    bigquery.SchemaField("price",                "STRING"),
    bigquery.SchemaField("date_of_transfer_raw", "STRING"),
    bigquery.SchemaField("postcode",             "STRING"),
    bigquery.SchemaField("property_type",        "STRING"),
    bigquery.SchemaField("old_new",              "STRING"),
    bigquery.SchemaField("duration",             "STRING"),
    bigquery.SchemaField("paon",                 "STRING"),
    bigquery.SchemaField("saon",                 "STRING"),
    bigquery.SchemaField("street",               "STRING"),
    bigquery.SchemaField("locality",             "STRING"),
    bigquery.SchemaField("town_city",            "STRING"),
    bigquery.SchemaField("district",             "STRING"),
    bigquery.SchemaField("county",               "STRING"),
    bigquery.SchemaField("ppd_category_type",    "STRING"),
    bigquery.SchemaField("record_status",        "STRING"),
]

# ─── DETERMINE RUN MONTH ──────────────────────────────────
def get_run_month():
    """
    Returns the month to ingest in YYYY-MM format.
    Can be overridden by passing a command line argument.
    """
    if len(sys.argv) > 1:
        return sys.argv[1]
    # Default: previous month
    today = date.today()
    if today.month == 1:
        return f"{today.year - 1}-12"
    return f"{today.year}-{today.month - 1:02d}"

# ─── DOWNLOAD ─────────────────────────────────────────────
def download_monthly_update(run_month: str) -> pd.DataFrame:
    url = (
        f"http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"
        f"/pp-monthly-update-{run_month}.csv"
    )
    print(f"Downloading: {url}")
    response = requests.get(url, timeout=120)

    if response.status_code == 404:
        print(f"No update file found for {run_month} — may not be published yet")
        sys.exit(0)

    response.raise_for_status()

    df = pd.read_csv(
        io.BytesIO(response.content),
        header=None,
        names=COLUMNS,
        dtype=str,
        na_filter=False
    )
    print(f"Downloaded {len(df):,} rows for {run_month}")
    return df

# ─── VALIDATE ─────────────────────────────────────────────
def validate(df: pd.DataFrame, run_month: str):
    if len(df) == 0:
        raise ValueError(f"Empty dataframe for {run_month} — aborting")

    null_ids = df["transaction_id"].isna().sum()
    if null_ids > 0:
        raise ValueError(f"{null_ids} null transaction_ids — aborting")

    print(f"Validation passed — {len(df):,} rows ready to load")

# ─── UPLOAD ───────────────────────────────────────────────
def upload_to_bigquery(df: pd.DataFrame, client: bigquery.Client):
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition="WRITE_APPEND",  # append monthly updates
    )

    job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=job_config
    )
    job.result()

    table = client.get_table(table_ref)
    print(f"Upload complete. Total rows in table: {table.num_rows:,}")

# ─── MAIN ─────────────────────────────────────────────────
def main():
    run_month = get_run_month()
    print(f"Running ingestion for: {run_month}")

    client = bigquery.Client(project=PROJECT_ID)

    df = download_monthly_update(run_month)
    validate(df, run_month)
    upload_to_bigquery(df, client)

    print(f"Ingestion complete for {run_month}")

if __name__ == "__main__":
    main()