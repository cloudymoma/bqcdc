#!/usr/bin/env python3
"""
Pub/Sub Initialization Script for BigQuery CDC Demo.

This script:
1. Creates the Pub/Sub topic if it doesn't exist
2. Creates the Pub/Sub subscription if it doesn't exist
3. Seeds initial 10 items as UPSERT events
"""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import yaml
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists, NotFound


SAMPLE_DESCRIPTIONS = [
    "Ergonomic Keyboard",
    "4K IPS Monitor",
    "Wireless Vertical Mouse",
    "USB-C Docking Station",
    "Noise-Cancelling Headphones",
    "Standing Desk Frame",
    "Webcam 1080p HD",
    "Microphone Arm Stand",
    "Mechanical Numpad",
    "Ultra-Wide Gaming Pad"
]


def load_config():
    """Load configuration from conf.yml."""
    config_path = Path(__file__).parent.parent / "conf.yml"
    print(f"[INFO] Loading configuration from {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_publisher_subscriber_clients(sa_path):
    """Initialize Pub/Sub publisher and subscriber clients."""
    if sa_path and os.path.exists(sa_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
        print(f"[INFO] Using service account: {sa_path}")
    else:
        if sa_path:
            print(f"[WARN] Service account not found at {sa_path}, using default credentials")
        else:
            print("[INFO] Using default application credentials")

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    return publisher, subscriber


def create_topic_if_not_exists(publisher, project_id, topic_name):
    """Idempotently create Pub/Sub topic."""
    topic_path = publisher.topic_path(project_id, topic_name)
    print(f"[INFO] Checking topic: {topic_path}")

    try:
        topic = publisher.get_topic(request={"topic": topic_path})
        print(f"[INFO] Topic '{topic_name}' already exists.")
        return topic_path
    except NotFound:
        print(f"[INFO] Topic '{topic_name}' not found. Creating...")
        try:
            topic = publisher.create_topic(request={"name": topic_path})
            print(f"[SUCCESS] Created topic: {topic.name}")
            return topic_path
        except AlreadyExists:
            print(f"[INFO] Topic '{topic_name}' was created concurrently.")
            return topic_path


def create_subscription_if_not_exists(subscriber, project_id, topic_path, sub_name, ack_deadline, retention_duration, retain_acked):
    """Idempotently create Pub/Sub subscription."""
    sub_path = subscriber.subscription_path(project_id, sub_name)
    print(f"[INFO] Checking subscription: {sub_path}")

    try:
        sub = subscriber.get_subscription(request={"subscription": sub_path})
        print(f"[INFO] Subscription '{sub_name}' already exists.")
        return sub_path
    except NotFound:
        print(f"[INFO] Subscription '{sub_name}' not found. Creating...")
        sub_request = {
            "name": sub_path,
            "topic": topic_path,
            "ack_deadline_seconds": ack_deadline,
            "retain_acked_messages": retain_acked,
        }
        if retention_duration:
            # Parse duration format like "604800s"
            duration_secs = int(str(retention_duration).rstrip("s"))
            sub_request["message_retention_duration"] = {"seconds": duration_secs}

        try:
            sub = subscriber.create_subscription(request=sub_request)
            print(f"[SUCCESS] Created subscription: {sub.name}")
            return sub_path
        except AlreadyExists:
            print(f"[INFO] Subscription '{sub_name}' was created concurrently.")
            return sub_path


def seed_initial_records(publisher, topic_path, count=10):
    """Publish initial seed records as UPSERT events."""
    print(f"\n[INFO] Publishing {count} initial seed records to {topic_path}...")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    now_millis = int(time.time() * 1000)

    for i in range(1, count + 1):
        desc = SAMPLE_DESCRIPTIONS[(i - 1) % len(SAMPLE_DESCRIPTIONS)]
        price = round(20.0 + (i * 15.5) % 150.0, 2)
        event = {
            "id": i,
            "description": desc,
            "price": price,
            "created_at": now_str,
            "updated_at": now_str,
            "_change_type": "UPSERT",
            "_sequence_number": now_millis + i
        }
        data = json.dumps(event).encode("utf-8")
        future = publisher.publish(topic_path, data)
        future.result(timeout=10)
        print(f"  [SEED] Published item id={i}, desc='{desc}', price=${price}, change_type=UPSERT")

    print(f"[SUCCESS] Seeded {count} initial records.")


def main():
    print("\n" + "=" * 60)
    print(" BigQuery CDC Demo - Pub/Sub Initialization")
    print("=" * 60 + "\n")

    config = load_config()

    sa_path = os.path.expanduser(config["gcp"].get("service_account_path", ""))
    project_id = config["gcp"]["project_id"]
    pubsub_cfg = config["pubsub"]
    topic_name = pubsub_cfg["topic_name"]
    sub_name = pubsub_cfg["subscription_name"]
    ack_deadline = pubsub_cfg.get("ack_deadline_seconds", 60)
    retention_duration = pubsub_cfg.get("message_retention_duration", "604800s")
    retain_acked = pubsub_cfg.get("retain_acked_messages", False)
    initial_count = config.get("generator", {}).get("initial_items_count", 10)

    publisher, subscriber = get_publisher_subscriber_clients(sa_path)

    topic_path = create_topic_if_not_exists(publisher, project_id, topic_name)
    sub_path = create_subscription_if_not_exists(
        subscriber, project_id, topic_path, sub_name, ack_deadline, retention_duration, retain_acked
    )

    seed_initial_records(publisher, topic_path, count=initial_count)

    print("\n" + "=" * 60)
    print(" Pub/Sub Initialization Complete!")
    print("=" * 60)
    print(f"  Topic:        {topic_path}")
    print(f"  Subscription: {sub_path}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[INFO] Operation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)
