#!/usr/bin/env python3
"""
MySQL Initialization Script for BigQuery CDC Demo.

This script:
1. Creates a Cloud SQL MySQL instance
2. Generates a complex root password
3. Makes the instance publicly accessible
4. Creates the database and table
5. Inserts 10 sample records
"""

import os
import sys
import secrets
import string
import subprocess
import time
from pathlib import Path

import yaml
import mysql.connector
from mysql.connector import Error as MySQLError


def load_config():
    """Load configuration from conf.yml."""
    config_path = Path(__file__).parent.parent / "conf.yml"
    print(f"[INFO] Loading configuration from {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def generate_password(length=24):
    """Generate a complex password with letters, digits, and special characters."""
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*()_+-="
    password = ''.join(secrets.choice(alphabet) for _ in range(length))
    # Ensure at least one of each type
    password = (
        secrets.choice(string.ascii_uppercase) +
        secrets.choice(string.ascii_lowercase) +
        secrets.choice(string.digits) +
        secrets.choice("!@#$%^&*()_+-=") +
        password[4:]
    )
    return password


def run_gcloud(args, check=True):
    """Run a gcloud command and return the output."""
    cmd = ["gcloud"] + args
    print(f"[CMD] {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"[ERROR] Command failed: {result.stderr}")
        raise RuntimeError(f"gcloud command failed: {result.stderr}")
    return result


def check_instance_exists(project_id, instance_name):
    """Check if Cloud SQL instance already exists."""
    result = run_gcloud([
        "sql", "instances", "describe", instance_name,
        "--project", project_id,
        "--format", "value(name)"
    ], check=False)
    return result.returncode == 0


def wait_for_instance_ready(project_id, instance_name, max_wait=600):
    """Wait for the Cloud SQL instance to be ready."""
    print(f"[INFO] Waiting for instance {instance_name} to be ready...")
    start_time = time.time()
    while time.time() - start_time < max_wait:
        result = run_gcloud([
            "sql", "instances", "describe", instance_name,
            "--project", project_id,
            "--format", "value(state)"
        ], check=False)
        state = result.stdout.strip()
        print(f"[INFO] Instance state: {state}")
        if state == "RUNNABLE":
            print("[INFO] Instance is ready!")
            return True
        time.sleep(10)
    raise TimeoutError("Instance did not become ready in time")


def get_instance_ip(project_id, instance_name):
    """Get the public IP of the Cloud SQL instance."""
    result = run_gcloud([
        "sql", "instances", "describe", instance_name,
        "--project", project_id,
        "--format", "value(ipAddresses[0].ipAddress)"
    ])
    ip = result.stdout.strip()
    print(f"[INFO] Instance IP: {ip}")
    return ip


def create_mysql_instance(config, password):
    """Create Cloud SQL MySQL instance."""
    project_id = config["gcp"]["project_id"]
    region = config["gcp"]["region"]
    instance_name = config["mysql"]["instance_name"]
    tier = config["mysql"]["tier"]
    version = config["mysql"]["version"]

    print(f"\n{'='*60}")
    print(f"Creating Cloud SQL MySQL Instance")
    print(f"{'='*60}")
    print(f"  Project: {project_id}")
    print(f"  Region: {region}")
    print(f"  Instance: {instance_name}")
    print(f"  Tier: {tier}")
    print(f"  Version: {version}")
    print(f"{'='*60}\n")

    # Check if instance exists
    if check_instance_exists(project_id, instance_name):
        print(f"[INFO] Instance {instance_name} already exists. Skipping creation.")
        # Update password
        print("[INFO] Updating root password...")
        run_gcloud([
            "sql", "users", "set-password", "root",
            "--host", "%",
            "--instance", instance_name,
            "--project", project_id,
            "--password", password
        ])
    else:
        # Create instance with development preset (no high availability, lower resources)
        print("[INFO] Creating new Cloud SQL instance (this may take several minutes)...")
        run_gcloud([
            "sql", "instances", "create", instance_name,
            "--project", project_id,
            "--region", region,
            "--database-version", version,
            "--tier", tier,
            "--root-password", password,
            "--edition", "enterprise",
            "--availability-type", "zonal",
            "--storage-type", "HDD",
            "--storage-size", "10",
            "--no-assign-ip",
            "--network", f"projects/{project_id}/global/networks/default",
        ])

        # Wait for instance to be ready
        wait_for_instance_ready(project_id, instance_name)

    # Add authorized networks (0.0.0.0/0 for demo - public access)
    print("[INFO] Configuring public access for demo purposes...")

    # First, patch to add public IP
    run_gcloud([
        "sql", "instances", "patch", instance_name,
        "--project", project_id,
        "--assign-ip",
        "--quiet"
    ])

    time.sleep(5)  # Wait for patch to apply

    # Add authorized network
    run_gcloud([
        "sql", "instances", "patch", instance_name,
        "--project", project_id,
        "--authorized-networks", "0.0.0.0/0",
        "--quiet"
    ])

    # Wait for instance to be ready again
    wait_for_instance_ready(project_id, instance_name)

    return get_instance_ip(project_id, instance_name)


def create_database_and_table(host, user, password, config):
    """Create database and table with sample data."""
    db_name = config["mysql"]["db_name"]
    table_name = config["mysql"]["table_name"]

    print(f"\n{'='*60}")
    print(f"Creating Database and Table")
    print(f"{'='*60}")
    print(f"  Host: {host}")
    print(f"  Database: {db_name}")
    print(f"  Table: {table_name}")
    print(f"{'='*60}\n")

    # Connect to MySQL
    print("[INFO] Connecting to MySQL...")
    max_retries = 10
    retry_delay = 10

    for attempt in range(max_retries):
        try:
            connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                connect_timeout=30
            )
            print("[INFO] Connected to MySQL!")
            break
        except MySQLError as e:
            print(f"[WARN] Connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                print(f"[INFO] Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise

    cursor = connection.cursor()

    try:
        # Create database
        print(f"[INFO] Creating database '{db_name}'...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
        cursor.execute(f"USE `{db_name}`")

        # Create table
        print(f"[INFO] Creating table '{table_name}'...")
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id INT PRIMARY KEY,
            description VARCHAR(255) NOT NULL,
            price DECIMAL(10, 2) NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_sql)

        # Check if data already exists
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        count = cursor.fetchone()[0]

        if count > 0:
            print(f"[INFO] Table already has {count} records. Clearing for fresh data...")
            cursor.execute(f"TRUNCATE TABLE `{table_name}`")

        # Insert 10 sample records
        print("[INFO] Inserting 10 sample records...")
        import random

        descriptions = [
            "Wireless Mouse", "Mechanical Keyboard", "USB-C Hub",
            "Laptop Stand", "Webcam HD", "Noise-Canceling Headphones",
            "Portable SSD", "Monitor Light Bar", "Ergonomic Chair",
            "Standing Desk Mat"
        ]

        for i in range(1, 11):
            price = round(random.uniform(10.0, 500.0), 2)
            desc = descriptions[i - 1]
            insert_sql = f"""
            INSERT INTO `{table_name}` (id, description, price)
            VALUES ({i}, '{desc}', {price})
            """
            cursor.execute(insert_sql)
            print(f"  [+] Inserted: id={i}, description='{desc}', price=${price:.2f}")

        connection.commit()
        print("[INFO] All records inserted successfully!")

        # Verify data
        print("\n[INFO] Verifying inserted data:")
        cursor.execute(f"SELECT * FROM `{table_name}`")
        rows = cursor.fetchall()
        print(f"\n{'='*80}")
        print(f"{'ID':>4} | {'Description':<30} | {'Price':>10} | {'Created At':<20} | {'Updated At':<20}")
        print(f"{'='*80}")
        for row in rows:
            print(f"{row[0]:>4} | {row[1]:<30} | ${row[2]:>9.2f} | {str(row[3]):<20} | {str(row[4]):<20}")
        print(f"{'='*80}\n")

    finally:
        cursor.close()
        connection.close()
        print("[INFO] MySQL connection closed.")


def main():
    """Main entry point."""
    print("\n" + "="*60)
    print(" BigQuery CDC Demo - MySQL Initialization")
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

    # Set project
    project_id = config["gcp"]["project_id"]
    run_gcloud(["config", "set", "project", project_id])

    # Generate password
    password = generate_password()
    print(f"[INFO] Generated root password (saved to mysql.password)")

    # Save password to file
    password_file = Path(__file__).parent.parent / "mysql.password"
    with open(password_file, "w") as f:
        f.write(password)
    os.chmod(password_file, 0o600)  # Restrict permissions
    print(f"[INFO] Password saved to {password_file}")

    # Create MySQL instance
    host = create_mysql_instance(config, password)

    # Create database and table
    create_database_and_table(
        host=host,
        user=config["mysql"]["root_user"],
        password=password,
        config=config
    )

    print("\n" + "="*60)
    print(" MySQL Initialization Complete!")
    print("="*60)
    print(f"\n  Instance: {config['mysql']['instance_name']}")
    print(f"  Host: {host}")
    print(f"  Database: {config['mysql']['db_name']}")
    print(f"  Table: {config['mysql']['table_name']}")
    print(f"  User: root")
    print(f"  Password: (saved in mysql.password)")
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
