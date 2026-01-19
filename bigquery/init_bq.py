#!/usr/bin/env python3
"""
BigQuery Initialization Script for BigQuery CDC Demo.

This script:
1. Creates the BigQuery dataset if it doesn't exist
2. Creates the table with the same schema as MySQL
"""

import os
import sys
from pathlib import Path

import yaml
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Conflict


def load_config():
    """Load configuration from conf.yml."""
    config_path = Path(__file__).parent.parent / "conf.yml"
    print(f"[INFO] Loading configuration from {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def create_dataset_if_not_exists(client, project_id, dataset_id, location):
    """Create BigQuery dataset if it doesn't exist."""
    dataset_ref = f"{project_id}.{dataset_id}"

    print(f"[INFO] Checking dataset: {dataset_ref}")

    try:
        client.get_dataset(dataset_ref)
        print(f"[INFO] Dataset '{dataset_id}' already exists.")
        return True
    except NotFound:
        print(f"[INFO] Dataset '{dataset_id}' not found. Creating...")

        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        dataset.description = "BigQuery CDC Demo Dataset"

        try:
            dataset = client.create_dataset(dataset, timeout=30)
            print(f"[INFO] Dataset '{dataset_id}' created successfully.")
            return True
        except Conflict:
            print(f"[INFO] Dataset '{dataset_id}' was created by another process.")
            return True


def create_table_if_not_exists(client, project_id, dataset_id, table_id):
    """Create BigQuery table with CDC schema and PRIMARY KEY if it doesn't exist.

    BigQuery CDC requires:
    1. A PRIMARY KEY constraint on the table
    2. The Storage Write API to be used for writes
    3. The _CHANGE_TYPE column in the data (UPSERT or DELETE)
    """
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    print(f"[INFO] Checking table: {table_ref}")

    try:
        client.get_table(table_ref)
        print(f"[INFO] Table '{table_id}' already exists.")
        return True
    except NotFound:
        print(f"[INFO] Table '{table_id}' not found. Creating with PRIMARY KEY...")

        # Use DDL to create table with PRIMARY KEY constraint
        # This is required for BigQuery CDC with UPSERT semantics
        ddl = f"""
        CREATE TABLE `{project_id}.{dataset_id}.{table_id}` (
            id INT64 NOT NULL,
            description STRING NOT NULL,
            price FLOAT64 NOT NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            PRIMARY KEY (id) NOT ENFORCED
        )
        CLUSTER BY id
        OPTIONS (
            description = 'CDC Target Table - Items from MySQL with PRIMARY KEY for UPSERT support'
        )
        """

        try:
            job = client.query(ddl)
            job.result()  # Wait for the query to complete
            print(f"[INFO] Table '{table_id}' created successfully with PRIMARY KEY.")
            print(f"[INFO] Schema:")
            print(f"  - id: INT64 (PRIMARY KEY)")
            print(f"  - description: STRING")
            print(f"  - price: FLOAT64")
            print(f"  - created_at: DATETIME")
            print(f"  - updated_at: DATETIME")
            return True
        except Exception as e:
            if "Already Exists" in str(e):
                print(f"[INFO] Table '{table_id}' was created by another process.")
                return True
            raise


def verify_table(client, project_id, dataset_id, table_id):
    """Verify table exists and show its details."""
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    try:
        table = client.get_table(table_ref)
        print(f"\n{'='*60}")
        print(f"Table Verification: {table_ref}")
        print(f"{'='*60}")
        print(f"  Created: {table.created}")
        print(f"  Modified: {table.modified}")
        print(f"  Rows: {table.num_rows}")
        print(f"  Size: {table.num_bytes} bytes")
        print(f"\n  Schema:")
        for field in table.schema:
            print(f"    - {field.name}: {field.field_type} ({field.mode})")
        print(f"{'='*60}\n")
        return True
    except NotFound:
        print(f"[ERROR] Table '{table_ref}' not found after creation!")
        return False


def main():
    """Main entry point."""
    print("\n" + "="*60)
    print(" BigQuery CDC Demo - BigQuery Initialization")
    print("="*60 + "\n")

    # Load configuration
    config = load_config()

    # Set up GCP credentials
    sa_path = os.path.expanduser(config["gcp"]["service_account_path"])
    if os.path.exists(sa_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
        print(f"[INFO] Using service account: {sa_path}")
    else:
        print(f"[WARN] Service account file not found: {sa_path}")
        print("[INFO] Using default credentials")

    # Get configuration values
    project_id = config["gcp"]["project_id"]
    dataset_id = config["bigquery"]["dataset"]
    table_id = config["bigquery"]["table_name"]
    location = config["bigquery"]["location"]

    print(f"\n[INFO] Configuration:")
    print(f"  Project: {project_id}")
    print(f"  Dataset: {dataset_id}")
    print(f"  Table: {table_id}")
    print(f"  Location: {location}")
    print()

    # Create BigQuery client
    client = bigquery.Client(project=project_id)

    # Create dataset
    if not create_dataset_if_not_exists(client, project_id, dataset_id, location):
        print("[ERROR] Failed to create dataset")
        sys.exit(1)

    # Create table
    if not create_table_if_not_exists(client, project_id, dataset_id, table_id):
        print("[ERROR] Failed to create table")
        sys.exit(1)

    # Verify
    verify_table(client, project_id, dataset_id, table_id)

    print("\n" + "="*60)
    print(" BigQuery Initialization Complete!")
    print("="*60)
    print(f"\n  Full table path: {project_id}.{dataset_id}.{table_id}")
    print("\n" + "="*60 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[INFO] Operation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)
