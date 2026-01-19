# BigQuery CDC Demo Makefile
# Controls all aspects of the MySQL to BigQuery CDC pipeline

SHELL := /bin/bash

# Python interpreter path - can be overridden by user
# Usage: make PYTHON3=/path/to/python3 <target>
PYTHON3 ?= python3

# Configuration file
CONFIG_FILE := conf.yml

# Extract configuration values using yq or Python
PROJECT_ID := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['gcp']['project_id'])")
REGION := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['gcp']['region'])")
SA_PATH := $(shell $(PYTHON3) -c "import yaml; import os; print(os.path.expanduser(yaml.safe_load(open('$(CONFIG_FILE)'))['gcp']['service_account_path']))")

MYSQL_INSTANCE := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['mysql']['instance_name'])")
MYSQL_DB := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['mysql']['db_name'])")
MYSQL_TABLE := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['mysql']['table_name'])")

BQ_DATASET := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['bigquery']['dataset'])")
BQ_TABLE := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['bigquery']['table_name'])")

DATAFLOW_JOB := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['dataflow']['job_name'])")
DATAFLOW_WORKERS := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['dataflow']['num_workers'])")
DATAFLOW_MACHINE := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['dataflow']['machine_type'])")

# Streaming CDC Configuration
POLLING_INTERVAL := $(shell $(PYTHON3) -c "import yaml; print(yaml.safe_load(open('$(CONFIG_FILE)'))['cdc']['polling_interval_seconds'])")
UPDATE_ALL_IF_TS_NULL := $(shell $(PYTHON3) -c "import yaml; print(str(yaml.safe_load(open('$(CONFIG_FILE)'))['cdc']['update_all_if_ts_null']).lower())")

# GCS bucket for Dataflow temp/staging
GCS_BUCKET := gs://$(PROJECT_ID)-dataflow-temp

# Python virtual environment
VENV_DIR := .venv

.PHONY: help setup install_deps init_mysql update_mysql init_bq build_dataflow run_cdc \
        clean cleanup_mysql cleanup_bq cleanup_dataflow cleanup_all status

# Default target
help:
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║           BigQuery CDC Demo - Available Commands                  ║"
	@echo "╠══════════════════════════════════════════════════════════════════╣"
	@echo "║  Setup & Installation:                                            ║"
	@echo "║    make setup          - Create virtual env and install deps      ║"
	@echo "║    make install_deps   - Install Python dependencies only         ║"
	@echo "║                                                                    ║"
	@echo "║  Main Operations:                                                  ║"
	@echo "║    make init_mysql     - Create Cloud SQL instance & seed data    ║"
	@echo "║    make update_mysql   - Continuously update MySQL records        ║"
	@echo "║    make init_bq        - Create BigQuery dataset and table        ║"
	@echo "║    make build_dataflow - Build Dataflow pipeline JAR              ║"
	@echo "║    make run_cdc        - Launch Dataflow CDC job                  ║"
	@echo "║                                                                    ║"
	@echo "║  Utilities:                                                        ║"
	@echo "║    make status         - Show status of all components            ║"
	@echo "║    make clean          - Remove build artifacts                   ║"
	@echo "║    make cleanup_mysql  - Delete Cloud SQL instance                ║"
	@echo "║    make cleanup_bq     - Delete BigQuery dataset                  ║"
	@echo "║    make cleanup_all    - Delete all GCP resources                 ║"
	@echo "║                                                                    ║"
	@echo "║  Variables (override with PYTHON3=/path/to/python):               ║"
	@echo "║    PYTHON3=$(PYTHON3)                                             ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "Current Configuration:"
	@echo "  Project:        $(PROJECT_ID)"
	@echo "  Region:         $(REGION)"
	@echo "  MySQL Instance: $(MYSQL_INSTANCE)"
	@echo "  BigQuery:       $(PROJECT_ID).$(BQ_DATASET).$(BQ_TABLE)"
	@echo "  Python:         $(PYTHON3)"
	@echo ""
	@echo "Streaming CDC Settings:"
	@echo "  Polling Interval:     $(POLLING_INTERVAL) seconds"
	@echo "  Update All If TS Null: $(UPDATE_ALL_IF_TS_NULL)"

# Setup virtual environment and install dependencies
# Uses --extra-index-url to ensure public PyPI is checked for packages
setup:
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║  Setting up Python virtual environment...                         ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	$(PYTHON3) -m venv $(VENV_DIR)
	$(VENV_DIR)/bin/pip install --upgrade pip
	$(VENV_DIR)/bin/pip install --extra-index-url https://pypi.org/simple/ -r mysql/requirements.txt
	$(VENV_DIR)/bin/pip install --extra-index-url https://pypi.org/simple/ -r bigquery/requirements.txt
	@echo ""
	@echo "[SUCCESS] Virtual environment created at $(VENV_DIR)"
	@echo "[INFO] Activate with: source $(VENV_DIR)/bin/activate"

# Install dependencies only (assumes venv is activated)
install_deps:
	@echo "[INFO] Installing Python dependencies..."
	pip install --extra-index-url https://pypi.org/simple/ -r mysql/requirements.txt
	pip install --extra-index-url https://pypi.org/simple/ -r bigquery/requirements.txt
	@echo "[SUCCESS] Dependencies installed"

# Initialize MySQL (Cloud SQL instance + database + table + seed data)
init_mysql:
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║  Initializing MySQL (Cloud SQL)                                   ║"
	@echo "╠══════════════════════════════════════════════════════════════════╣"
	@echo "║  This will:                                                       ║"
	@echo "║    1. Create Cloud SQL MySQL 8.0 instance                         ║"
	@echo "║    2. Generate and save root password                             ║"
	@echo "║    3. Configure public access                                     ║"
	@echo "║    4. Create database and table                                   ║"
	@echo "║    5. Insert 10 sample records                                    ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	@if [ -f "$(VENV_DIR)/bin/python" ]; then \
		GOOGLE_APPLICATION_CREDENTIALS="$(SA_PATH)" $(VENV_DIR)/bin/python mysql/init_mysql.py; \
	else \
		GOOGLE_APPLICATION_CREDENTIALS="$(SA_PATH)" $(PYTHON3) mysql/init_mysql.py; \
	fi

# Continuously update MySQL records
update_mysql:
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║  Starting MySQL Continuous Update                                 ║"
	@echo "║  Press Ctrl+C to stop                                             ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	@if [ -f "$(VENV_DIR)/bin/python" ]; then \
		GOOGLE_APPLICATION_CREDENTIALS="$(SA_PATH)" $(VENV_DIR)/bin/python mysql/update_mysql.py; \
	else \
		GOOGLE_APPLICATION_CREDENTIALS="$(SA_PATH)" $(PYTHON3) mysql/update_mysql.py; \
	fi

# Initialize BigQuery (dataset + table)
init_bq:
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║  Initializing BigQuery                                            ║"
	@echo "╠══════════════════════════════════════════════════════════════════╣"
	@echo "║  This will:                                                       ║"
	@echo "║    1. Create dataset if not exists                                ║"
	@echo "║    2. Create table with matching schema                           ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	@if [ -f "$(VENV_DIR)/bin/python" ]; then \
		GOOGLE_APPLICATION_CREDENTIALS="$(SA_PATH)" $(VENV_DIR)/bin/python bigquery/init_bq.py; \
	else \
		GOOGLE_APPLICATION_CREDENTIALS="$(SA_PATH)" $(PYTHON3) bigquery/init_bq.py; \
	fi

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

# Get MySQL instance IP
MYSQL_IP := $(shell gcloud sql instances describe $(MYSQL_INSTANCE) --project=$(PROJECT_ID) --format="value(ipAddresses[0].ipAddress)" 2>/dev/null || echo "UNKNOWN")

# Run Dataflow CDC job (supports --update for existing jobs)
run_cdc: create_gcs_bucket
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║  Launching Dataflow CDC Job                                       ║"
	@echo "╠══════════════════════════════════════════════════════════════════╣"
	@echo "║  Job Name:     $(DATAFLOW_JOB)                                    ║"
	@echo "║  Workers:      $(DATAFLOW_WORKERS) x $(DATAFLOW_MACHINE)          ║"
	@echo "║  Source:       MySQL $(MYSQL_INSTANCE)                            ║"
	@echo "║  Destination:  BigQuery $(BQ_DATASET).$(BQ_TABLE)                 ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	@if [ ! -f "mysql.password" ]; then \
		echo "[ERROR] mysql.password not found. Run 'make init_mysql' first."; \
		exit 1; \
	fi
	@MYSQL_PASSWORD=$$(cat mysql.password); \
	MYSQL_HOST=$$(gcloud sql instances describe $(MYSQL_INSTANCE) --project=$(PROJECT_ID) --format="value(ipAddresses[0].ipAddress)"); \
	echo "[INFO] MySQL Host: $$MYSQL_HOST"; \
	UPDATE_FLAG=""; \
	EXISTING_JOB=$$(gcloud dataflow jobs list --project=$(PROJECT_ID) --region=$(REGION) \
		--filter="name=$(DATAFLOW_JOB) AND (state=Running OR state=Pending OR state=Queued)" \
		--format="value(id)" --limit=1 2>/dev/null); \
	if [ -n "$$EXISTING_JOB" ]; then \
		echo "[INFO] Found existing job: $$EXISTING_JOB - will update in-place"; \
		UPDATE_FLAG="--update"; \
	else \
		echo "[INFO] No existing job found - creating new job"; \
	fi; \
	echo "[INFO] Starting Dataflow job..."; \
	cd dataflow && mvn exec:java -Pdataflow \
		-Dexec.mainClass=com.bindiego.cdc.CdcPipeline \
		-Dexec.args="--project=$(PROJECT_ID) --region=$(REGION) --runner=DataflowRunner --jobName=$(DATAFLOW_JOB) --streaming=true --experiments=enable_streaming_engine --numWorkers=$(DATAFLOW_WORKERS) --maxNumWorkers=$(DATAFLOW_WORKERS) --workerMachineType=$(DATAFLOW_MACHINE) --gcpTempLocation=$(GCS_BUCKET)/temp --stagingLocation=$(GCS_BUCKET)/staging --mysqlJdbcUrl=jdbc:mysql://$$MYSQL_HOST:3306 --mysqlUsername=root --mysqlPassword=$$MYSQL_PASSWORD --mysqlDatabase=$(MYSQL_DB) --mysqlTable=$(MYSQL_TABLE) --bigQueryTable=$(PROJECT_ID):$(BQ_DATASET).$(BQ_TABLE) --gcsTempLocation=$(GCS_BUCKET)/bq-temp --pollingIntervalSeconds=$(POLLING_INTERVAL) --updateAllIfTsNull=$(UPDATE_ALL_IF_TS_NULL) $$UPDATE_FLAG"

# Show status of all components
status:
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║  BigQuery CDC Demo - Component Status                             ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "MySQL (Cloud SQL):"
	@echo "  Instance: $(MYSQL_INSTANCE)"
	@gcloud sql instances describe $(MYSQL_INSTANCE) --project=$(PROJECT_ID) \
		--format="table(state,ipAddresses[0].ipAddress,settings.tier)" 2>/dev/null || \
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
	@echo "Dataflow Jobs:"
	@gcloud dataflow jobs list --project=$(PROJECT_ID) --region=$(REGION) \
		--filter="name:$(DATAFLOW_JOB)" --limit=3 2>/dev/null || \
		echo "  No jobs found"

# Clean build artifacts
clean:
	@echo "[INFO] Cleaning build artifacts..."
	cd dataflow && mvn clean 2>/dev/null || true
	rm -rf $(VENV_DIR)
	rm -rf __pycache__ mysql/__pycache__ bigquery/__pycache__
	rm -f *.pyc mysql/*.pyc bigquery/*.pyc
	@echo "[SUCCESS] Cleaned"

# Cleanup MySQL instance
cleanup_mysql:
	@echo "[WARN] This will DELETE the Cloud SQL instance: $(MYSQL_INSTANCE)"
	@read -p "Are you sure? (yes/no): " confirm && \
	if [ "$$confirm" = "yes" ]; then \
		gcloud sql instances delete $(MYSQL_INSTANCE) --project=$(PROJECT_ID) --quiet; \
		rm -f mysql.password; \
		echo "[SUCCESS] MySQL instance deleted"; \
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
	@for job_id in $$(gcloud dataflow jobs list --project=$(PROJECT_ID) --region=$(REGION) \
		--filter="name:$(DATAFLOW_JOB) AND state:Running" --format="value(id)"); do \
		echo "Cancelling job: $$job_id"; \
		gcloud dataflow jobs cancel $$job_id --project=$(PROJECT_ID) --region=$(REGION); \
	done
	@echo "[SUCCESS] Dataflow jobs cancelled"

# Cleanup all resources
cleanup_all: cleanup_dataflow cleanup_bq cleanup_mysql
	@echo ""
	@echo "[SUCCESS] All resources cleaned up"
	-gsutil rm -r $(GCS_BUCKET) 2>/dev/null || true
