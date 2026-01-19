#!/usr/bin/env python3
"""
MySQL Update Script for BigQuery CDC Demo.

This script continuously updates random records in the MySQL table
every 1-3 seconds until Ctrl+C is pressed.
"""

import os
import sys
import random
import signal
import time
import subprocess
from datetime import datetime
from pathlib import Path

import yaml
import mysql.connector
from mysql.connector import Error as MySQLError


# Global flag for graceful shutdown
running = True


def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully."""
    global running
    print("\n[INFO] Shutdown signal received. Stopping...")
    running = False


def load_config():
    """Load configuration from conf.yml."""
    config_path = Path(__file__).parent.parent / "conf.yml"
    print(f"[INFO] Loading configuration from {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def load_password():
    """Load MySQL password from file."""
    password_file = Path(__file__).parent.parent / "mysql.password"
    if not password_file.exists():
        raise FileNotFoundError(
            f"Password file not found: {password_file}\n"
            "Please run 'make init_mysql' first."
        )
    with open(password_file, "r") as f:
        return f.read().strip()


def get_instance_ip(project_id, instance_name):
    """Get the public IP of the Cloud SQL instance."""
    cmd = [
        "gcloud", "sql", "instances", "describe", instance_name,
        "--project", project_id,
        "--format", "value(ipAddresses[0].ipAddress)"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to get instance IP: {result.stderr}")
    ip = result.stdout.strip()
    print(f"[INFO] Instance IP: {ip}")
    return ip


def update_random_record(cursor, table_name):
    """Update a random record with new price."""
    # Pick a random ID between 1 and 10
    record_id = random.randint(1, 10)

    # Get current price
    cursor.execute(f"SELECT price, description FROM `{table_name}` WHERE id = {record_id}")
    result = cursor.fetchone()
    if not result:
        print(f"[WARN] Record {record_id} not found")
        return None

    old_price = float(result[0])
    description = result[1]

    # Generate new random price
    new_price = round(random.uniform(10.0, 500.0), 2)

    # Update the record with new price and current timestamp
    cursor.execute(f"""
        UPDATE `{table_name}`
        SET price = {new_price}, updated_at = NOW()
        WHERE id = {record_id}
    """)

    return {
        "id": record_id,
        "description": description,
        "old_price": old_price,
        "new_price": new_price,
        "timestamp": datetime.now()
    }


def main():
    """Main entry point."""
    global running

    # Set up signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("\n" + "="*70)
    print(" BigQuery CDC Demo - MySQL Continuous Update")
    print("="*70)
    print(" Press Ctrl+C to stop")
    print("="*70 + "\n")

    # Load configuration
    config = load_config()

    # Set up GCP credentials
    sa_path = os.path.expanduser(config["gcp"]["service_account_path"])
    if os.path.exists(sa_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path

    # Get instance IP
    project_id = config["gcp"]["project_id"]
    instance_name = config["mysql"]["instance_name"]
    host = get_instance_ip(project_id, instance_name)

    # Load password
    password = load_password()

    # Get table info
    db_name = config["mysql"]["db_name"]
    table_name = config["mysql"]["table_name"]

    print(f"[INFO] Connecting to MySQL...")
    print(f"  Host: {host}")
    print(f"  Database: {db_name}")
    print(f"  Table: {table_name}")
    print()

    # Connect to MySQL
    connection = mysql.connector.connect(
        host=host,
        user=config["mysql"]["root_user"],
        password=password,
        database=db_name,
        connect_timeout=30,
        autocommit=True
    )
    cursor = connection.cursor()

    print("[INFO] Connected! Starting continuous updates...\n")
    print("-"*70)
    print(f"{'Time':<12} | {'ID':>3} | {'Item':<30} | {'Old Price':>10} | {'New Price':>10}")
    print("-"*70)

    update_count = 0

    try:
        while running:
            # Update a random record
            result = update_random_record(cursor, table_name)

            if result:
                update_count += 1
                time_str = result["timestamp"].strftime("%H:%M:%S")
                print(
                    f"{time_str:<12} | "
                    f"{result['id']:>3} | "
                    f"{result['description']:<30} | "
                    f"${result['old_price']:>9.2f} | "
                    f"${result['new_price']:>9.2f}"
                )

            # Wait random time between 1-3 seconds
            wait_time = random.uniform(1.0, 3.0)
            time.sleep(wait_time)

    except MySQLError as e:
        print(f"\n[ERROR] MySQL error: {e}")
    finally:
        cursor.close()
        connection.close()
        print("\n" + "-"*70)
        print(f"[INFO] Total updates: {update_count}")
        print("[INFO] MySQL connection closed.")
        print("[INFO] Goodbye!\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)
