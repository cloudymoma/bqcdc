# BigQuery CDC Demo Makefile
# Controls all aspects of the Pub/Sub to BigQuery CDC pipeline

SHELL := /bin/bash

# Python interpreter path - can be overridden by user
# Usage: make PYTHON3=/path/to/python3 <target>
PYTHON3 ?= python3

# Configuration file
CONFIG_FILE := conf.yml

# Extract configuration values using yq or Python (suppress errors if dependencies not yet installed)
PROJECT_ID := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['gcp']['project_id'])" 2>/dev/null)
REGION := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['gcp']['region'])" 2>/dev/null)
SA_PATH := $(shell $(PYTHON3) -c "import yaml; import os; print(os.path.expanduser(yaml.safe_load(open('$(CONFIG_FILE)'))['gcp']['service_account_path']))" 2>/dev/null)

PUBSUB_TOPIC := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['pubsub']['topic_name'])" 2>/dev/null)
PUBSUB_SUB := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['pubsub']['subscription_name'])" 2>/dev/null)

BQ_DATASET := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['bigquery']['dataset'])" 2>/dev/null)
BQ_TABLE := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['bigquery']['table_name'])" 2>/dev/null)

DATAFLOW_JOB := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['dataflow']['job_name'])" 2>/dev/null)
DATAFLOW_WORKERS := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['dataflow']['num_workers'])" 2>/dev/null)
DATAFLOW_MACHINE := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['dataflow']['machine_type'])" 2>/dev/null)

# GCS bucket for Dataflow temp/staging
GCS_BUCKET := gs://$(PROJECT_ID)-dataflow-temp

# Python virtual environment
VENV_DIR := .venv

.PHONY: help setup install_deps init_pubsub stream_pubsub init_bq test build_dataflow \
        create_gcs_bucket run_cdc clean cleanup_pubsub cleanup_bq cleanup_dataflow cleanup_all status

# Default target
help:
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║       BigQuery CDC Demo (Pub/Sub) - Available Commands            ║"
	@echo "╠══════════════════════════════════════════════════════════════════╣"
	@echo "║  Setup & Installation:                                            ║"
	@echo "║    make setup           - Create virtual env and install deps     ║"
	@echo "║    make install_deps    - Install Python dependencies only        ║"
	@echo "║                                                                    ║"
	@echo "║  Main Operations:                                                  ║"
	@echo "║    make init_pubsub     - Create Pub/Sub topic & sub, seed data   ║"
	@echo "║    make stream_pubsub   - Continuously publish CDC events         ║"
	@echo "║    make init_bq         - Create BigQuery dataset and table       ║"
	@echo "║    make test            - Run pipeline unit tests                 ║"
	@echo "║    make build_dataflow  - Build Dataflow pipeline JAR             ║"
	@echo "║    make run_cdc         - Launch Dataflow CDC job                 ║"
	@echo "║                                                                    ║"
	@echo "║  Utilities:                                                        ║"
	@echo "║    make status          - Show status of all components           ║"
	@echo "║    make clean           - Remove build artifacts                  ║"
	@echo "║    make cleanup_pubsub  - Delete Pub/Sub topic & subscription     ║"
	@echo "║    make cleanup_bq      - Delete BigQuery dataset                 ║"
	@echo "║    make cleanup_dataflow - Cancel running Dataflow jobs           ║"
	@echo "║    make cleanup_all     - Delete all GCP resources                ║"
	@echo "║                                                                    ║"
	@echo "║  Variables (override with PYTHON3=/path/to/python):               ║"
	@echo "║    PYTHON3=$(PYTHON3)                                             ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "Current Configuration:"
	@echo "  Project:        $(PROJECT_ID)"
	@echo "  Region:         $(REGION)"
	@echo "  Pub/Sub Topic:  $(PUBSUB_TOPIC)"
	@echo "  Pub/Sub Sub:    $(PUBSUB_SUB)"
	@echo "  BigQuery:       $(PROJECT_ID).$(BQ_DATASET).$(BQ_TABLE)"
	@echo "  Dataflow Job:   $(DATAFLOW_JOB)"
	@echo "  Python:         $(PYTHON3)"

# Setup virtual environment and install dependencies
# Uses --extra-index-url to ensure public PyPI is checked for packages
setup:
	@if [ ! -f "$(VENV_DIR)/bin/python" ]; then \
		echo "╔══════════════════════════════════════════════════════════════════╗"; \
		echo "║  Setting up Python virtual environment...                         ║"; \
		echo "╚══════════════════════════════════════════════════════════════════╝"; \
		$(PYTHON3) -m venv $(VENV_DIR); \
		$(VENV_DIR)/bin/pip install --upgrade pip; \
		echo ""; \
		echo "[SUCCESS] Virtual environment created at $(VENV_DIR)"; \
		echo "[INFO] Activate with: source $(VENV_DIR)/bin/activate"; \
	fi
	@$(VENV_DIR)/bin/pip install -q --extra-index-url https://pypi.org/simple/ -r pubsub/requirements.txt -r bigquery/requirements.txt

# Install dependencies only (assumes venv is activated)
install_deps:
	@echo "[INFO] Installing Python dependencies..."
	pip install --extra-index-url https://pypi.org/simple/ -r pubsub/requirements.txt
	pip install --extra-index-url https://pypi.org/simple/ -r bigquery/requirements.txt
	@echo "[SUCCESS] Dependencies installed"

# Initialize Pub/Sub (topic + subscription + seed data)
init_pubsub: setup
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║  Initializing Pub/Sub                                             ║"
	@echo "╠══════════════════════════════════════════════════════════════════╣"
	@echo "║  This will:                                                       ║"
	@echo "║    1. Create Pub/Sub topic if not exists                          ║"
	@echo "║    2. Create Pub/Sub subscription if not exists                   ║"
	@echo "║    3. Publish 10 initial seed records (UPSERT)                    ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	@if [ -f "$(VENV_DIR)/bin/python" ]; then \
		GOOGLE_APPLICATION_CREDENTIALS="$(SA_PATH)" $(VENV_DIR)/bin/python pubsub/init_pubsub.py; \
	else \
		GOOGLE_APPLICATION_CREDENTIALS="$(SA_PATH)" $(PYTHON3) pubsub/init_pubsub.py; \
	fi

# Continuously publish CDC events to Pub/Sub
stream_pubsub: setup
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║  Starting Pub/Sub Streaming Event Generator                       ║"
	@echo "║  Press Ctrl+C to stop                                             ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	@if [ -f "$(VENV_DIR)/bin/python" ]; then \
		GOOGLE_APPLICATION_CREDENTIALS="$(SA_PATH)" $(VENV_DIR)/bin/python pubsub/stream_publisher.py; \
	else \
		GOOGLE_APPLICATION_CREDENTIALS="$(SA_PATH)" $(PYTHON3) pubsub/stream_publisher.py; \
	fi

# Initialize BigQuery (dataset + table)
init_bq: setup
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║  Initializing BigQuery                                            ║"
	@echo "╠══════════════════════════════════════════════════════════════════╣"
	@echo "║  This will:                                                       ║"
	@echo "║    1. Create dataset if not exists                                ║"
	@echo "║    2. Create table with PRIMARY KEY (id) NOT ENFORCED             ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	@if [ -f "$(VENV_DIR)/bin/python" ]; then \
		GOOGLE_APPLICATION_CREDENTIALS="$(SA_PATH)" $(VENV_DIR)/bin/python bigquery/init_bq.py; \
	else \
		GOOGLE_APPLICATION_CREDENTIALS="$(SA_PATH)" $(PYTHON3) bigquery/init_bq.py; \
	fi

# Run unit tests
test:
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║  Running Unit Tests                                               ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	cd dataflow && mvn test
	@echo ""
	@echo "[SUCCESS] All tests passed!"

# Build Dataflow pipeline JAR
build_dataflow:
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║  Building Dataflow Pipeline                                       ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	cd dataflow && mvn clean package -DskipTests -Pdataflow
	@echo ""
	@echo "[SUCCESS] JAR built at dataflow/target/bqcdc-dataflow-1.0.0.jar"

# Create GCS bucket for Dataflow temp storage
create_gcs_bucket:
	@echo "[INFO] Creating GCS bucket for Dataflow: $(GCS_BUCKET)"
	-gsutil mb -p $(PROJECT_ID) -l $(REGION) $(GCS_BUCKET) 2>/dev/null || true

# Run Dataflow CDC job (supports --update for existing jobs)
run_cdc: create_gcs_bucket
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║  Launching Dataflow CDC Job                                       ║"
	@echo "╠══════════════════════════════════════════════════════════════════╣"
	@echo "║  Job Name:     $(DATAFLOW_JOB)                                    ║"
	@echo "║  Workers:      $(DATAFLOW_WORKERS) x $(DATAFLOW_MACHINE)          ║"
	@echo "║  Source:       Pub/Sub $(PUBSUB_SUB)                              ║"
	@echo "║  Destination:  BigQuery $(BQ_DATASET).$(BQ_TABLE)                 ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	@UPDATE_FLAG=""; \
	EXISTING_JOB=$$(gcloud dataflow jobs list --project=$(PROJECT_ID) --billing-project=$(PROJECT_ID) --region=$(REGION) \
		--filter="name=$(DATAFLOW_JOB) AND (state=Running OR state=Pending OR state=Queued)" \
		--format="value(id)" --limit=1 --quiet 2>/dev/null); \
	if [ -n "$$EXISTING_JOB" ]; then \
		echo "[INFO] Found existing job: $$EXISTING_JOB - will update in-place"; \
		UPDATE_FLAG="--update"; \
	else \
		echo "[INFO] No existing job found - creating new job"; \
	fi; \
	if [ -f "$(SA_PATH)" ]; then \
		export GOOGLE_APPLICATION_CREDENTIALS="$(SA_PATH)"; \
	fi; \
	echo "[INFO] Starting Dataflow job..."; \
	cd dataflow && mvn exec:java -Pdataflow \
		-Dexec.mainClass=com.bindiego.cdc.CdcPipeline \
		-Dexec.args="--project=$(PROJECT_ID) --region=$(REGION) --runner=DataflowRunner --jobName=$(DATAFLOW_JOB) --streaming=true --experiments=enable_streaming_engine --numWorkers=$(DATAFLOW_WORKERS) --maxNumWorkers=$(DATAFLOW_WORKERS) --workerMachineType=$(DATAFLOW_MACHINE) --gcpTempLocation=$(GCS_BUCKET)/temp --stagingLocation=$(GCS_BUCKET)/staging --pubsubSubscription=projects/$(PROJECT_ID)/subscriptions/$(PUBSUB_SUB) --bigQueryTable=$(PROJECT_ID):$(BQ_DATASET).$(BQ_TABLE) --gcsTempLocation=$(GCS_BUCKET)/bq-temp $$UPDATE_FLAG"

# Show status of all components
status:
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║  BigQuery CDC Demo - Component Status                             ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "Pub/Sub:"
	@echo "  Topic: $(PUBSUB_TOPIC)"
	@gcloud pubsub topics describe projects/$(PROJECT_ID)/topics/$(PUBSUB_TOPIC) \
		--project=$(PROJECT_ID) --billing-project=$(PROJECT_ID) --format="value(name)" 2>/dev/null || \
		echo "  Status: NOT FOUND"
	@echo ""
	@echo "  Subscription: $(PUBSUB_SUB)"
	@gcloud pubsub subscriptions describe projects/$(PROJECT_ID)/subscriptions/$(PUBSUB_SUB) \
		--project=$(PROJECT_ID) --billing-project=$(PROJECT_ID) \
		--format="table(name,topic,ackDeadlineSeconds)" 2>/dev/null || \
		echo "  Status: NOT FOUND"
	@echo ""
	@echo "BigQuery:"
	@echo "  Dataset: $(PROJECT_ID).$(BQ_DATASET)"
	@bq show --project_id=$(PROJECT_ID) $(BQ_DATASET) 2>/dev/null | head -5 || \
		echo "  Status: NOT FOUND"
	@echo ""
	@echo "  Table: $(BQ_TABLE)"
	@bq show --project_id=$(PROJECT_ID) $(BQ_DATASET).$(BQ_TABLE) 2>/dev/null | head -10 || \
		echo "  Status: NOT FOUND"
	@echo ""
	@echo "  Row count:"
	@bq query --project_id=$(PROJECT_ID) --use_legacy_sql=false --format=pretty \
		"SELECT COUNT(*) AS row_count FROM \`$(PROJECT_ID).$(BQ_DATASET).$(BQ_TABLE)\`" 2>/dev/null || \
		echo "  Status: NOT AVAILABLE"
	@echo ""
	@echo "Dataflow Jobs:"
	@gcloud dataflow jobs list --project=$(PROJECT_ID) --billing-project=$(PROJECT_ID) --region=$(REGION) --quiet \
		--filter="name:$(DATAFLOW_JOB)" --limit=3 2>/dev/null || \
		echo "  No jobs found"

# Clean build artifacts
clean:
	@echo "[INFO] Cleaning build artifacts..."
	cd dataflow && mvn clean 2>/dev/null || true
	rm -rf $(VENV_DIR)
	rm -rf __pycache__ pubsub/__pycache__ bigquery/__pycache__
	rm -f *.pyc pubsub/*.pyc bigquery/*.pyc
	@echo "[SUCCESS] Cleaned"

# Cleanup Pub/Sub topic and subscription
cleanup_pubsub:
	@echo "[WARN] This will DELETE the Pub/Sub topic '$(PUBSUB_TOPIC)' and subscription '$(PUBSUB_SUB)'"
	@read -p "Are you sure? (yes/no): " confirm && \
	if [ "$$confirm" = "yes" ]; then \
		gcloud pubsub subscriptions delete projects/$(PROJECT_ID)/subscriptions/$(PUBSUB_SUB) \
			--project=$(PROJECT_ID) --billing-project=$(PROJECT_ID) --quiet 2>/dev/null || true; \
		gcloud pubsub topics delete projects/$(PROJECT_ID)/topics/$(PUBSUB_TOPIC) \
			--project=$(PROJECT_ID) --billing-project=$(PROJECT_ID) --quiet 2>/dev/null || true; \
		echo "[SUCCESS] Pub/Sub topic and subscription deleted"; \
	else \
		echo "[INFO] Cancelled"; \
	fi

# Cleanup BigQuery dataset
cleanup_bq:
	@echo "[WARN] This will DELETE the BigQuery dataset: $(BQ_DATASET)"
	@read -p "Are you sure? (yes/no): " confirm && \
	if [ "$$confirm" = "yes" ]; then \
		bq rm -r -f --project_id=$(PROJECT_ID) $(BQ_DATASET); \
		echo "[SUCCESS] BigQuery dataset deleted"; \
	else \
		echo "[INFO] Cancelled"; \
	fi

# Cleanup Dataflow jobs
cleanup_dataflow:
	@echo "[INFO] Cancelling Dataflow jobs..."
	@for job_id in $$(gcloud dataflow jobs list --project=$(PROJECT_ID) --billing-project=$(PROJECT_ID) --region=$(REGION) --quiet \
		--filter="name:$(DATAFLOW_JOB) AND state:Running" --format="value(id)" 2>/dev/null); do \
		echo "Cancelling job: $$job_id"; \
		gcloud dataflow jobs cancel $$job_id --project=$(PROJECT_ID) --billing-project=$(PROJECT_ID) --region=$(REGION) --quiet; \
	done
	@echo "[SUCCESS] Dataflow jobs cancelled"

# Cleanup all resources
cleanup_all: cleanup_dataflow cleanup_bq cleanup_pubsub
	@echo ""
	@echo "[SUCCESS] All resources cleaned up"
	-gsutil rm -r $(GCS_BUCKET) 2>/dev/null || true
