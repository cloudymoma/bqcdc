# BigQuery CDC Demo

A complete demonstration of Change Data Capture (CDC) from MySQL to BigQuery using Google Cloud Platform services.

## Architecture

![BigQuery CDC Architecture](miscs/bqcdc_arch.png)

The pipeline continuously polls MySQL for changes based on the `updated_at` column and syncs modified records to BigQuery using the Storage Write API with UPSERT semantics.

## Components

| Component | Description |
|-----------|-------------|
| **MySQL (Cloud SQL)** | Source database with sample item table |
| **Dataflow Pipeline** | Java/Apache Beam pipeline for CDC processing |
| **BigQuery** | Destination data warehouse |

## Prerequisites

Before starting, ensure you have:

1. **Google Cloud SDK** installed and configured
   ```bash
   gcloud --version
   ```

2. **Java 11+** and **Maven 3.6+** for Dataflow pipeline
   ```bash
   java -version
   mvn -version
   ```

3. **Python 3.8+** for MySQL and BigQuery scripts
   ```bash
   python3 --version
   ```

4. **GCP Project** with the following APIs enabled:
   - Cloud SQL Admin API
   - BigQuery API
   - Dataflow API
   - Compute Engine API

5. **Service Account** with appropriate permissions:
   - Cloud SQL Admin
   - BigQuery Admin
   - Dataflow Admin
   - Storage Admin

## Quick Start

### Step 1: Clone and Configure

```bash
# Navigate to project directory
cd bqcdc

# Review and edit configuration (optional)
# Default values work out of the box
cat conf.yml
```

### Step 2: Setup Environment

```bash
# Create virtual environment and install dependencies
make setup

# Or if you prefer to install dependencies globally
make install_deps
```

### Step 3: Initialize MySQL

```bash
# Create Cloud SQL instance, database, and seed data
# This may take 5-10 minutes for instance creation
make init_mysql
```

**What this does:**
- Creates Cloud SQL MySQL 8.0 instance
- Generates a secure root password (saved to `mysql.password`)
- Configures public access for demo purposes
- Creates `dingocdc` database with `item` table
- Inserts 10 sample records

### Step 4: Initialize BigQuery

```bash
# Create BigQuery dataset and table
make init_bq
```

**What this does:**
- Creates `dingocdc` dataset
- Creates `item` table with matching schema

### Step 5: Build Dataflow Pipeline

```bash
# Build the Java pipeline JAR
make build_dataflow
```

### Step 6: Start the CDC Pipeline

Open **Terminal 1** - Start the Dataflow job:
```bash
make run_cdc
```

### Step 7: Generate Data Changes

Open **Terminal 2** - Start continuous updates:
```bash
make update_mysql
```

This will randomly update item prices every 1-3 seconds until you press Ctrl+C.

### Step 8: Verify in BigQuery

```bash
# Query the BigQuery table to see synced data
bq query --project_id=du-hast-mich \
  "SELECT * FROM dingocdc.item ORDER BY updated_at DESC LIMIT 10"
```

Or use the BigQuery Console in GCP.

## Configuration Reference

Edit `conf.yml` to customize:

```yaml
gcp:
  project_id: "du-hast-mich"          # Your GCP project ID
  region: "us-central1"                # GCP region
  service_account_path: "~/workspace/google/sa.json"

mysql:
  instance_name: "dingomysql"          # Cloud SQL instance name
  db_name: "dingocdc"                  # Database name
  table_name: "item"                   # Table name
  tier: "db-f1-micro"                  # Machine type

bigquery:
  dataset: "dingocdc"                  # BigQuery dataset
  table_name: "item"                   # BigQuery table
  location: "US"                       # Dataset location

dataflow:
  job_name: "dingo-cdc"                # Dataflow job name
  num_workers: 1                       # Initial number of workers
  max_workers: 2                       # Maximum number of workers
  machine_type: "e2-medium"            # Worker machine type

cdc:
  polling_interval_seconds: 10         # How often to poll for changes
  update_all_if_ts_null: true          # Startup behavior (see below)
```

### Streaming CDC Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `polling_interval_seconds` | 10 | How often (in seconds) the pipeline polls MySQL for changes |
| `update_all_if_ts_null` | true | Controls behavior on pipeline startup: `true` = full table sync first, `false` = only capture new changes |

## Make Targets

| Target | Description |
|--------|-------------|
| `make help` | Show all available commands |
| `make setup` | Create virtual env and install dependencies |
| `make init_mysql` | Create Cloud SQL instance and seed data |
| `make update_mysql` | Start continuous MySQL updates |
| `make init_bq` | Create BigQuery dataset and table |
| `make build_dataflow` | Build Dataflow pipeline JAR |
| `make run_cdc` | Launch Dataflow CDC job |
| `make status` | Show status of all components |
| `make cleanup_all` | Delete all GCP resources |

### Custom Python Path

To use a custom Python interpreter, set the `PYTHON3` variable:

```bash
# Use a specific Python version
make PYTHON3=/usr/bin/python3.11 setup

# Use pyenv Python
make PYTHON3=~/.pyenv/shims/python3 init_mysql

# Use conda Python
make PYTHON3=/opt/conda/bin/python3 init_bq
```

## Table Schema

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key (1-10) |
| `description` | STRING | Item description |
| `price` | FLOAT | Item price (randomly updated) |
| `created_at` | DATETIME | Record creation timestamp |
| `updated_at` | DATETIME | Last update timestamp |

## Project Structure

```
bqcdc/
├── conf.yml                    # Configuration file
├── Makefile                    # Build and run automation
├── README.md                   # This file
├── mysql.password              # Generated MySQL password (gitignored)
├── .gitignore                  # Git ignore rules
│
├── mysql/                      # MySQL-related scripts
│   ├── init_mysql.py           # Initialize Cloud SQL instance
│   ├── update_mysql.py         # Continuous update script
│   └── requirements.txt        # Python dependencies
│
├── bigquery/                   # BigQuery-related scripts
│   ├── init_bq.py              # Initialize BigQuery
│   └── requirements.txt        # Python dependencies
│
└── dataflow/                   # Dataflow pipeline (Java/Maven)
    ├── pom.xml                 # Maven configuration
    └── src/main/java/com/bindiego/cdc/
        ├── CdcPipeline.java            # Streaming CDC pipeline with UPSERT
        └── CdcPipelineOptions.java     # Pipeline options interface
```

## Streaming CDC Logic

This pipeline implements a **stateful streaming CDC** approach using Apache Beam's `ValueState` to track the last processed `updated_at` timestamp in memory. The pipeline runs continuously and polls MySQL at a configurable interval.

### How It Works

```
┌────────────────────────────────────────────────────────────────────────────┐
│                        STREAMING CDC FLOW DIAGRAM                          │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌─────────────────┐                                                       │
│  │  Pipeline Start │                                                       │
│  └────────┬────────┘                                                       │
│           │                                                                │
│           ▼                                                                │
│  ┌─────────────────────┐                                                   │
│  │ Is lastTimestamp    │                                                   │
│  │ NULL? (First Poll)  │                                                   │
│  └────────┬────────────┘                                                   │
│           │                                                                │
│     ┌─────┴─────┐                                                          │
│     │           │                                                          │
│    YES         NO                                                          │
│     │           │                                                          │
│     ▼           │                                                          │
│  ┌──────────────────────┐                                                  │
│  │ updateAllIfTsNull?   │                                                  │
│  └──────────┬───────────┘                                                  │
│             │                                                              │
│      ┌──────┴──────┐                              ┌────────────────────┐   │
│      │             │                              │                    │   │
│    TRUE          FALSE                            │                    │   │
│      │             │                              │                    ▼   │
│      ▼             ▼                              │  ┌─────────────────────┐│
│  ┌───────────┐  ┌───────────────────┐             │  │ Query records WHERE ││
│  │ FULL SYNC │  │ Query MAX         │             │  │ updated_at >        ││
│  │           │  │ (updated_at)      │             │  │ lastTimestamp       ││
│  │ Query ALL │  │                   │             │  └──────────┬──────────┘│
│  │ records   │  │ Record timestamp  │             │             │           │
│  │           │  │ (no data sync)    │             │             ▼           │
│  │ Sync to   │  │                   │             │  ┌─────────────────────┐│
│  │ BigQuery  │  │ Wait for next     │             │  │ Sync changed records││
│  │           │  │ poll              │             │  │ to BigQuery         ││
│  │ Record    │  └───────────────────┘             │  └──────────┬──────────┘│
│  │ max(ts)   │                                    │             │           │
│  └───────────┘                                    │             ▼           │
│                                                   │  ┌─────────────────────┐│
│                                                   │  │ Update lastTimestamp││
│                                                   │  │ to max(updated_at)  ││
│                                                   │  └──────────┬──────────┘│
│                                                   │             │           │
│                                                   └─────────────┤           │
│                                                                 ▼           │
│                                                   ┌─────────────────────┐   │
│                                                   │ Wait polling_interval│   │
│                                                   │ seconds, then repeat │   │
│                                                   └─────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────┘
```

### Detailed Logic

#### Scenario 1: First Poll with `lastTimestamp = NULL`

**Case A: `update_all_if_ts_null = true` (Full Table Sync)**

1. Query: `SELECT * FROM table ORDER BY updated_at`
2. Emit ALL records to BigQuery
3. Record `max(updated_at)` as the new `lastTimestamp`
4. Subsequent polls: Only capture records newer than `lastTimestamp`

**Use Case**: When you need existing data in BigQuery before capturing changes.

**Case B: `update_all_if_ts_null = false` (Incremental Only)**

1. Query: `SELECT MAX(updated_at) FROM table`
2. Record this timestamp as `lastTimestamp`
3. Emit NOTHING to BigQuery (no initial sync)
4. Wait for next poll to capture new changes

**Use Case**: When you only want to capture NEW changes going forward, ignoring existing data.

#### Scenario 2: Subsequent Polls with `lastTimestamp != NULL`

For all polls after the first:

1. Query: `SELECT * FROM table WHERE updated_at > lastTimestamp ORDER BY updated_at`
2. Emit only the changed records to BigQuery
3. Update `lastTimestamp` to `max(updated_at)` from the fetched records
4. If no changes found, keep the existing `lastTimestamp`

### Example Workflow

```
Time    Event                           lastTimestamp    BigQuery Action
─────   ─────                           ─────────────    ──────────────
T0      Pipeline starts                 NULL             -
        (update_all_if_ts_null=false)
        Query MAX(updated_at)=10:00:00
        Record timestamp                10:00:00         Nothing synced

T1      Poll #1 (10 sec later)          10:00:00         -
        Query WHERE updated_at > 10:00
        No records found                10:00:00         Nothing synced

T2      MySQL UPDATE item SET           -                -
        price=99.99 WHERE id=5
        (updated_at = 10:00:15)

T3      Poll #2 (10 sec later)          10:00:00         -
        Query WHERE updated_at > 10:00
        Found 1 record (id=5)
        Sync to BigQuery                10:00:15         1 record inserted
        Update timestamp

T4      Poll #3 (10 sec later)          10:00:15         -
        Query WHERE updated_at > 10:00:15
        No records found                10:00:15         Nothing synced
```

### Important Notes

1. **BigQuery CDC with UPSERT**: This pipeline uses BigQuery's native CDC feature with the Storage Write API (`STORAGE_API_AT_LEAST_ONCE` method). It uses `RowMutationInformation` with `MutationType.UPSERT` to update existing rows by primary key (`id`) rather than appending new rows. The `updated_at` timestamp is used as the sequence number for CDC ordering.

2. **Primary Key Required**: The BigQuery table must have a PRIMARY KEY constraint on the `id` column. The `init_bq.py` script creates the table with `PRIMARY KEY (id) NOT ENFORCED`.

3. **State Persistence**: The `lastTimestamp` is stored in Beam's state backend. If the pipeline restarts, the state may be lost depending on the runner configuration. For production, consider persisting the watermark to an external store.

4. **Timestamp Precision**: Uses `>` (greater than) comparison to avoid re-processing records with the exact same timestamp. Ensure your `updated_at` column has sufficient precision (milliseconds recommended).

5. **Single-Threaded Polling**: Uses a single key ("cdc-poller") to ensure all state is managed in one place. This serializes the polling but guarantees consistency.

## Limitations of This Demo

**Important**: This demo uses the `updated_at` column to identify data changes, which has limitations:

1. **No DELETE detection**: Records deleted from MySQL will NOT be detected or removed from BigQuery. The polling approach only sees records that exist with `updated_at > lastTimestamp`.

2. **Requires timestamp column**: Your source table must have a reliably updated timestamp column.

3. **Higher latency**: Changes are detected on a polling interval (default 10 seconds), not in real-time.

**For production use cases**, consider using **binlog-based CDC** solutions like:
- **Google Datastream** - Managed CDC service that reads MySQL binlog
- **Debezium + Pub/Sub** - Open-source binlog parser with message queue

These solutions capture INSERT, UPDATE, and DELETE operations in real-time.

**However, the primary purpose of this demo is to illustrate how to write Apache Beam/Dataflow code with BigQuery's native CDC (Storage Write API with UPSERT semantics)**, not to provide a production-ready CDC solution. The polling mechanism is intentionally simple to keep the focus on the Dataflow pipeline implementation.

## CDC Approaches Comparison

| Approach | Pros | Cons |
|----------|------|------|
| **This Demo (Polling + Storage Write API CDC)** | True UPSERT semantics, no binlog access needed, uses native BigQuery CDC, simple to understand | No DELETE support, higher latency, depends on `updated_at` column |
| **Google Datastream** | Managed, real-time, binlog-based, supports DELETE | Additional service cost |
| **Debezium + Pub/Sub** | Real-time, open-source, supports DELETE | Complex setup, requires binlog access |

## Troubleshooting

### MySQL Connection Issues

```bash
# Check if instance is running
gcloud sql instances describe dingomysql --format="value(state)"

# Verify public IP access
gcloud sql instances describe dingomysql --format="value(ipAddresses)"

# Check authorized networks
gcloud sql instances describe dingomysql --format="value(settings.ipConfiguration.authorizedNetworks)"
```

### BigQuery Issues

```bash
# List datasets
bq ls --project_id=du-hast-mich

# Describe table
bq show du-hast-mich:dingocdc.item
```

### Dataflow Issues

```bash
# List running jobs
gcloud dataflow jobs list --region=us-central1 --filter="state:Running"

# View job logs
gcloud dataflow jobs show JOB_ID --region=us-central1
```

## Cleanup

To remove all GCP resources created by this demo:

```bash
# Cancel Dataflow jobs, delete BigQuery dataset, and Cloud SQL instance
make cleanup_all
```

Or individually:
```bash
make cleanup_dataflow  # Cancel Dataflow jobs
make cleanup_bq        # Delete BigQuery dataset
make cleanup_mysql     # Delete Cloud SQL instance
```

## Cost Considerations

This demo uses minimal resources:
- **Cloud SQL**: `db-f1-micro` (~$9/month if running 24/7)
- **Dataflow**: 1-2x `e2-medium` workers with Streaming Engine (pay per use)
- **BigQuery**: Pay per query/storage

**Recommendation**: Run `make cleanup_all` when done to avoid charges.

## Technical Requirements

### Dataflow Pipeline Dependencies

| Dependency | Version | Notes |
|------------|---------|-------|
| Apache Beam | 2.70.0 | Core streaming framework |
| google-auth-library | 1.34.0+ | Required for mTLS support (CertificateSourceUnavailableException) |
| MySQL Connector/J | 8.0.33 | JDBC driver for MySQL |
| Java | 11+ | Runtime requirement |

### Key Features Used

- **Streaming Engine**: Enabled via `--experiments=enable_streaming_engine` for better resource utilization
- **Storage Write API**: Uses `STORAGE_API_AT_LEAST_ONCE` method for CDC writes
- **Stateful Processing**: Uses Beam's `ValueState` for tracking last processed timestamp
- **CDC with Primary Key**: BigQuery table uses `PRIMARY KEY (id) NOT ENFORCED` for UPSERT semantics

## License

MIT License - Feel free to use and modify.
