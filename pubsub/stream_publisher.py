#!/usr/bin/env python3
"""
Streaming CDC Event Generator for BigQuery CDC Demo.

This script continuously generates and publishes CDC events (UPSERT and DELETE)
to the configured Google Cloud Pub/Sub topic until Ctrl+C is pressed.
"""

import json
import os
import random
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

import yaml
from google.cloud import pubsub_v1


# Global running flag for graceful shutdown
running = True

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
    "Ultra-Wide Gaming Pad",
    "Studio Monitor Speakers",
    "Thunderbolt 4 Cable",
    "Dual Monitor Arm Mount",
    "Blue Light Blocking Glasses",
    "Desk LED Light Bar"
]


def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully."""
    global running
    print("\n[INFO] Shutdown signal received. Exiting generator...")
    running = False


def load_config():
    """Load configuration from conf.yml."""
    config_path = Path(__file__).parent.parent / "conf.yml"
    print(f"[INFO] Loading configuration from {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("\n" + "=" * 60)
    print(" BigQuery CDC Demo - Pub/Sub Streaming Event Generator")
    print("=" * 60 + "\n")

    config = load_config()

    sa_path = os.path.expanduser(config["gcp"].get("service_account_path", ""))
    if sa_path and os.path.exists(sa_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
        print(f"[INFO] Using service account: {sa_path}")
    else:
        print("[INFO] Using default application credentials")

    project_id = config["gcp"]["project_id"]
    topic_name = config["pubsub"]["topic_name"]
    gen_cfg = config.get("generator", {})
    min_interval = gen_cfg.get("interval_min_seconds", 1.0)
    max_interval = gen_cfg.get("interval_max_seconds", 3.0)
    delete_ratio = gen_cfg.get("delete_ratio", 0.15)
    initial_count = gen_cfg.get("initial_items_count", 10)

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    # Active pool of items: {id: {"description": str, "price": float, "created_at": str}}
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    active_items = {
        i: {
            "description": SAMPLE_DESCRIPTIONS[(i - 1) % len(SAMPLE_DESCRIPTIONS)],
            "price": round(20.0 + (i * 15.5) % 150.0, 2),
            "created_at": now_str
        }
        for i in range(1, initial_count + 1)
    }
    next_item_id = initial_count + 1

    print(f"[INFO] Publishing events to {topic_path}")
    print(f"[INFO] Interval: {min_interval}s - {max_interval}s | Delete Ratio: {int(delete_ratio * 100)}%")
    print("[INFO] Press Ctrl+C to stop stream generator.\n")

    event_count = 0

    while running:
        try:
            sleep_duration = random.uniform(min_interval, max_interval)
            time.sleep(sleep_duration)

            if not running:
                break

            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            now_millis = int(time.time() * 1000)

            # Decide whether to perform DELETE or UPSERT
            # Only DELETE if there are active items
            is_delete = (random.random() < delete_ratio) and len(active_items) > 3

            if is_delete:
                # Pick a random item to delete
                item_id = random.choice(list(active_items.keys()))
                item_data = active_items.pop(item_id)

                event = {
                    "id": item_id,
                    "description": item_data["description"],
                    "price": item_data["price"],
                    "created_at": item_data["created_at"],
                    "updated_at": now_str,
                    "_change_type": "DELETE",
                    "_sequence_number": now_millis
                }
                op_label = "\033[91m[DELETE]\033[0m"
            else:
                # 70% chance to update an existing item, 30% chance to insert a new one
                if active_items and random.random() < 0.7:
                    item_id = random.choice(list(active_items.keys()))
                    # Update price or description
                    new_price = round(random.uniform(10.0, 500.0), 2)
                    active_items[item_id]["price"] = new_price
                    item_data = active_items[item_id]
                else:
                    item_id = next_item_id
                    next_item_id += 1
                    desc = random.choice(SAMPLE_DESCRIPTIONS)
                    new_price = round(random.uniform(10.0, 500.0), 2)
                    item_data = {
                        "description": desc,
                        "price": new_price,
                        "created_at": now_str
                    }
                    active_items[item_id] = item_data

                event = {
                    "id": item_id,
                    "description": item_data["description"],
                    "price": item_data["price"],
                    "created_at": item_data["created_at"],
                    "updated_at": now_str,
                    "_change_type": "UPSERT",
                    "_sequence_number": now_millis
                }
                op_label = "\033[92m[UPSERT]\033[0m"

            data = json.dumps(event).encode("utf-8")
            future = publisher.publish(topic_path, data)
            future.result(timeout=10)

            event_count += 1
            print(f"[#{event_count:04d}] {op_label} ID={event['id']:<3} | Desc='{event['description']:<26}' | Price=${event['price']:<7.2f} | Seq={event['_sequence_number']}")

        except Exception as e:
            if running:
                print(f"[WARN] Error publishing event: {e}")
                time.sleep(1)

    print(f"\n[SUCCESS] Stream generator stopped. Total events published: {event_count}")


if __name__ == "__main__":
    main()
