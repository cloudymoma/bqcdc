# BigQuery CDC 演示项目

[![Build](https://github.com/cloudymoma/bqcdc/actions/workflows/build.yml/badge.svg)](https://github.com/cloudymoma/bqcdc/actions/workflows/build.yml)

[English](README.md) | 中文

一个完整的变更数据捕获（CDC）演示项目，使用 Google Cloud Platform 服务将数据从 MySQL 同步到 BigQuery。

## 架构

![BigQuery CDC 架构图](miscs/bqcdc_arch.png)

该管道持续轮询 MySQL 中基于 `updated_at` 列的变更，并使用 Storage Write API 的 UPSERT 语义将修改的记录同步到 BigQuery。

> **重要提示**：本项目旨在演示**如何使用 Dataflow 通过 Storage Write API 的 UPSERT 语义将数据 CDC 到 BigQuery**。这不是一个生产就绪的 MySQL CDC 解决方案。
>
> 对于需要处理 INSERT、UPDATE 和 DELETE 操作的生产环境 MySQL CDC，您应该使用基于 binlog 的解决方案，例如：
> - [Debezium with Dataflow](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/master/v2/cdc-parent#deploying-the-connector) - 读取 MySQL binlog 进行实时变更捕获
> - [Google Datastream](https://cloud.google.com/datastream) - MySQL 到 BigQuery 的托管 CDC 服务
>
> 本演示仅定期读取 MySQL 表，完全依赖 `updated_at` 列进行变更检测，**无法检测 DELETE 操作**。

## 组件

| 组件 | 描述 |
|------|------|
| **MySQL (Cloud SQL)** | 包含示例 item 表的源数据库 |
| **Dataflow Pipeline** | 用于 CDC 处理的 Java/Apache Beam 管道 |
| **BigQuery** | 目标数据仓库 |

## 前置条件

在开始之前，请确保您已具备：

1. **Google Cloud SDK** 已安装并配置
   ```bash
   gcloud --version
   ```

2. **Java 11+** 和 **Maven 3.6+** 用于 Dataflow 管道
   ```bash
   java -version
   mvn -version
   ```

3. **Python 3.8+** 用于 MySQL 和 BigQuery 脚本
   ```bash
   python3 --version
   ```

4. **GCP 项目** 并启用以下 API：
   - Cloud SQL Admin API
   - BigQuery API
   - Dataflow API
   - Compute Engine API

5. **服务账号** 并具备相应权限：
   - Cloud SQL Admin
   - BigQuery Admin
   - Dataflow Admin
   - Storage Admin

## 快速开始

### 步骤 1：克隆并配置

```bash
# 进入项目目录
cd bqcdc

# 查看并编辑配置文件（可选）
# 默认值开箱即用
cat conf.yml
```

### 步骤 2：设置环境

```bash
# 创建虚拟环境并安装依赖
make setup

# 或者如果您希望全局安装依赖
make install_deps
```

### 步骤 3：初始化 MySQL

```bash
# 创建 Cloud SQL 实例、数据库和种子数据
# 实例创建可能需要 5-10 分钟
make init_mysql
```

**此步骤执行的操作：**
- 创建 Cloud SQL MySQL 8.0 实例
- 生成安全的 root 密码（保存到 `mysql.password`）
- 配置公网访问（仅用于演示目的）
- 创建 `dingocdc` 数据库和 `item` 表
- 插入 10 条示例记录

### 步骤 4：初始化 BigQuery

```bash
# 创建 BigQuery 数据集和表
make init_bq
```

**此步骤执行的操作：**
- 创建 `dingocdc` 数据集
- 创建具有匹配 schema 的 `item` 表

### 步骤 5：构建 Dataflow 管道

```bash
# 构建 Java 管道 JAR 包
make build_dataflow
```

### 步骤 6：启动 CDC 管道

打开 **终端 1** - 启动 Dataflow 作业：
```bash
make run_cdc
```

### 步骤 7：生成数据变更

打开 **终端 2** - 开始持续更新：
```bash
make update_mysql
```

这将每 1-3 秒随机更新商品价格，直到您按 Ctrl+C 停止。

### 步骤 8：在 BigQuery 中验证

```bash
# 查询 BigQuery 表以查看同步的数据
bq query --project_id=du-hast-mich \
  "SELECT * FROM dingocdc.item ORDER BY updated_at DESC LIMIT 10"
```

或者使用 GCP 控制台中的 BigQuery Console。

## 配置参考

编辑 `conf.yml` 进行自定义：

```yaml
gcp:
  project_id: "du-hast-mich"          # 您的 GCP 项目 ID
  region: "us-central1"                # GCP 区域
  service_account_path: "~/workspace/google/sa.json"

mysql:
  instance_name: "dingomysql"          # Cloud SQL 实例名称
  db_name: "dingocdc"                  # 数据库名称
  table_name: "item"                   # 表名称
  tier: "db-f1-micro"                  # 机器类型

bigquery:
  dataset: "dingocdc"                  # BigQuery 数据集
  table_name: "item"                   # BigQuery 表
  location: "US"                       # 数据集位置

dataflow:
  job_name: "dingo-cdc"                # Dataflow 作业名称
  num_workers: 1                       # 初始 worker 数量
  max_workers: 2                       # 最大 worker 数量
  machine_type: "e2-medium"            # Worker 机器类型

cdc:
  polling_interval_seconds: 10         # 轮询变更的频率
  update_all_if_ts_null: true          # 启动行为（见下文）
```

### 流式 CDC 配置选项

| 选项 | 默认值 | 描述 |
|------|--------|------|
| `polling_interval_seconds` | 10 | 管道轮询 MySQL 变更的频率（秒） |
| `update_all_if_ts_null` | true | 控制管道启动时的行为：`true` = 先全表同步，`false` = 仅捕获新变更 |

## Make 目标

| 目标 | 描述 |
|------|------|
| `make help` | 显示所有可用命令 |
| `make setup` | 创建虚拟环境并安装依赖 |
| `make init_mysql` | 创建 Cloud SQL 实例并填充数据 |
| `make update_mysql` | 开始持续更新 MySQL |
| `make init_bq` | 创建 BigQuery 数据集和表 |
| `make build_dataflow` | 构建 Dataflow 管道 JAR |
| `make run_cdc` | 启动 Dataflow CDC 作业 |
| `make status` | 显示所有组件状态 |
| `make cleanup_all` | 删除所有 GCP 资源 |

### 自定义 Python 路径

要使用自定义 Python 解释器，设置 `PYTHON3` 变量：

```bash
# 使用特定 Python 版本
make PYTHON3=/usr/bin/python3.11 setup

# 使用 pyenv Python
make PYTHON3=~/.pyenv/shims/python3 init_mysql

# 使用 conda Python
make PYTHON3=/opt/conda/bin/python3 init_bq
```

## 表结构

| 列名 | 类型 | 描述 |
|------|------|------|
| `id` | INTEGER | 主键 (1-10) |
| `description` | STRING | 商品描述 |
| `price` | FLOAT | 商品价格（随机更新） |
| `created_at` | DATETIME | 记录创建时间戳 |
| `updated_at` | DATETIME | 最后更新时间戳 |

## 项目结构

```
bqcdc/
├── conf.yml                    # 配置文件
├── Makefile                    # 构建和运行自动化
├── README.md                   # 英文说明文档
├── README_cn.md                # 中文说明文档（本文件）
├── mysql.password              # 生成的 MySQL 密码（已加入 gitignore）
├── .gitignore                  # Git 忽略规则
│
├── mysql/                      # MySQL 相关脚本
│   ├── init_mysql.py           # 初始化 Cloud SQL 实例
│   ├── update_mysql.py         # 持续更新脚本
│   └── requirements.txt        # Python 依赖
│
├── bigquery/                   # BigQuery 相关脚本
│   ├── init_bq.py              # 初始化 BigQuery
│   └── requirements.txt        # Python 依赖
│
└── dataflow/                   # Dataflow 管道（Java/Maven）
    ├── pom.xml                 # Maven 配置
    └── src/main/java/com/bindiego/cdc/
        ├── CdcPipeline.java            # 带 UPSERT 的流式 CDC 管道
        └── CdcPipelineOptions.java     # 管道选项接口
```

## 流式 CDC 逻辑

该管道使用 Apache Beam 的 `ValueState` 实现**有状态流式 CDC**方法，在内存中跟踪最后处理的 `updated_at` 时间戳。管道持续运行，并以可配置的间隔轮询 MySQL。

### 工作原理

```
┌────────────────────────────────────────────────────────────────────────────┐
│                        流式 CDC 流程图                                      │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌─────────────────┐                                                       │
│  │   管道启动      │                                                       │
│  └────────┬────────┘                                                       │
│           │                                                                │
│           ▼                                                                │
│  ┌─────────────────────┐                                                   │
│  │ lastTimestamp       │                                                   │
│  │ 为空？（首次轮询） │                                                   │
│  └────────┬────────────┘                                                   │
│           │                                                                │
│     ┌─────┴─────┐                                                          │
│     │           │                                                          │
│    是          否                                                          │
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
│  ┌───────────┐  ┌───────────────────┐             │  │ 查询 updated_at >   ││
│  │ 全量同步  │  │ 查询 MAX          │             │  │ lastTimestamp      ││
│  │           │  │ (updated_at)      │             │  │ 的记录             ││
│  │ 查询所有  │  │                   │             │  └──────────┬──────────┘│
│  │ 记录      │  │ 记录时间戳        │             │             │           │
│  │           │  │ （不同步数据）    │             │             ▼           │
│  │ 同步到    │  │                   │             │  ┌─────────────────────┐│
│  │ BigQuery  │  │ 等待下次轮询      │             │  │ 将变更的记录同步到 ││
│  │           │  │                   │             │  │ BigQuery           ││
│  │ 记录      │  └───────────────────┘             │  └──────────┬──────────┘│
│  │ max(ts)   │                                    │             │           │
│  └───────────┘                                    │             ▼           │
│                                                   │  ┌─────────────────────┐│
│                                                   │  │ 更新 lastTimestamp  ││
│                                                   │  │ 为 max(updated_at)  ││
│                                                   │  └──────────┬──────────┘│
│                                                   │             │           │
│                                                   └─────────────┤           │
│                                                                 ▼           │
│                                                   ┌─────────────────────┐   │
│                                                   │ 等待 polling_interval│   │
│                                                   │ 秒后重复            │   │
│                                                   └─────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────┘
```

### 详细逻辑

#### 场景 1：首次轮询时 `lastTimestamp = NULL`

**情况 A：`update_all_if_ts_null = true`（全表同步）**

1. 查询：`SELECT * FROM table ORDER BY updated_at`
2. 将所有记录发送到 BigQuery
3. 记录 `max(updated_at)` 作为新的 `lastTimestamp`
4. 后续轮询：仅捕获比 `lastTimestamp` 更新的记录

**使用场景**：当您需要在捕获变更之前先将现有数据同步到 BigQuery。

**情况 B：`update_all_if_ts_null = false`（仅增量）**

1. 查询：`SELECT MAX(updated_at) FROM table`
2. 将此时间戳记录为 `lastTimestamp`
3. 不向 BigQuery 发送任何数据（无初始同步）
4. 等待下次轮询以捕获新变更

**使用场景**：当您只想捕获未来的新变更，忽略现有数据。

#### 场景 2：后续轮询时 `lastTimestamp != NULL`

对于首次之后的所有轮询：

1. 查询：`SELECT * FROM table WHERE updated_at > lastTimestamp ORDER BY updated_at`
2. 仅将变更的记录发送到 BigQuery
3. 将 `lastTimestamp` 更新为获取记录中的 `max(updated_at)`
4. 如果未发现变更，保持现有的 `lastTimestamp`

### 示例工作流程

```
时间    事件                              lastTimestamp    BigQuery 操作
─────   ─────                             ─────────────    ──────────────
T0      管道启动                          NULL             -
        (update_all_if_ts_null=false)
        查询 MAX(updated_at)=10:00:00
        记录时间戳                        10:00:00         未同步任何数据

T1      轮询 #1（10 秒后）                10:00:00         -
        查询 WHERE updated_at > 10:00
        未找到记录                        10:00:00         未同步任何数据

T2      MySQL UPDATE item SET             -                -
        price=99.99 WHERE id=5
        (updated_at = 10:00:15)

T3      轮询 #2（10 秒后）                10:00:00         -
        查询 WHERE updated_at > 10:00
        找到 1 条记录 (id=5)
        同步到 BigQuery                   10:00:15         插入 1 条记录
        更新时间戳

T4      轮询 #3（10 秒后）                10:00:15         -
        查询 WHERE updated_at > 10:00:15
        未找到记录                        10:00:15         未同步任何数据
```

### 重要说明

1. **BigQuery CDC 与 UPSERT**：该管道使用 BigQuery 原生 CDC 功能与 Storage Write API（`STORAGE_API_AT_LEAST_ONCE` 方法）。它使用 `RowMutationInformation` 与 `MutationType.UPSERT` 通过主键（`id`）更新现有行，而不是追加新行。`updated_at` 时间戳用作 CDC 排序的序列号。

2. **需要主键**：BigQuery 表必须在 `id` 列上有 PRIMARY KEY 约束。`init_bq.py` 脚本创建的表带有 `PRIMARY KEY (id) NOT ENFORCED`。

3. **状态持久化**：`lastTimestamp` 存储在 Beam 的状态后端中。如果管道重启，根据 runner 配置，状态可能会丢失。对于生产环境，建议将水位线持久化到外部存储。

4. **时间戳精度**：使用 `>`（大于）比较以避免重复处理具有相同时间戳的记录。确保您的 `updated_at` 列具有足够的精度（建议毫秒级）。

5. **单线程轮询**：使用单个 key（"cdc-poller"）确保所有状态在一个地方管理。这会序列化轮询但保证一致性。

## 本演示的局限性

**重要提示**：本演示使用 `updated_at` 列来识别数据变更，这有一些局限性：

1. **无法检测 DELETE**：从 MySQL 删除的记录不会被检测到或从 BigQuery 中移除。轮询方法只能看到 `updated_at > lastTimestamp` 的现有记录。

2. **需要时间戳列**：您的源表必须有一个可靠更新的时间戳列。

3. **较高延迟**：变更是按轮询间隔（默认 10 秒）检测的，而非实时。

**对于生产用例**，建议使用基于 **binlog 的 CDC** 解决方案，例如：
- **Google Datastream** - 读取 MySQL binlog 的托管 CDC 服务
- **Debezium + Pub/Sub** - 开源 binlog 解析器配合消息队列

这些解决方案可以实时捕获 INSERT、UPDATE 和 DELETE 操作。

**然而，本演示的主要目的是说明如何使用 BigQuery 原生 CDC（Storage Write API 与 UPSERT 语义）编写 Apache Beam/Dataflow 代码**，而不是提供生产就绪的 CDC 解决方案。轮询机制故意保持简单，以便将重点放在 Dataflow 管道实现上。

## CDC 方法对比

| 方法 | 优点 | 缺点 |
|------|------|------|
| **本演示（轮询 + Storage Write API CDC）** | 真正的 UPSERT 语义，无需 binlog 访问，使用原生 BigQuery CDC，易于理解 | 不支持 DELETE，延迟较高，依赖 `updated_at` 列 |
| **Google Datastream** | 托管服务，实时，基于 binlog，支持 DELETE | 额外的服务成本 |
| **Debezium + Pub/Sub** | 实时，开源，支持 DELETE | 设置复杂，需要 binlog 访问权限 |

## 故障排除

### MySQL 连接问题

```bash
# 检查实例是否运行
gcloud sql instances describe dingomysql --format="value(state)"

# 验证公网 IP 访问
gcloud sql instances describe dingomysql --format="value(ipAddresses)"

# 检查授权网络
gcloud sql instances describe dingomysql --format="value(settings.ipConfiguration.authorizedNetworks)"
```

### BigQuery 问题

```bash
# 列出数据集
bq ls --project_id=du-hast-mich

# 描述表
bq show du-hast-mich:dingocdc.item
```

### Dataflow 问题

```bash
# 列出运行中的作业
gcloud dataflow jobs list --region=us-central1 --filter="state:Running"

# 查看作业日志
gcloud dataflow jobs show JOB_ID --region=us-central1
```

## 清理

要删除本演示创建的所有 GCP 资源：

```bash
# 取消 Dataflow 作业，删除 BigQuery 数据集和 Cloud SQL 实例
make cleanup_all
```

或者单独清理：
```bash
make cleanup_dataflow  # 取消 Dataflow 作业
make cleanup_bq        # 删除 BigQuery 数据集
make cleanup_mysql     # 删除 Cloud SQL 实例
```

## 成本考虑

本演示使用最小资源：
- **Cloud SQL**：`db-f1-micro`（如果 24/7 运行，约 $9/月）
- **Dataflow**：1-2 个 `e2-medium` worker，带 Streaming Engine（按使用付费）
- **BigQuery**：按查询/存储付费

**建议**：完成后运行 `make cleanup_all` 以避免产生费用。

## 技术要求

### Dataflow 管道依赖

| 依赖 | 版本 | 说明 |
|------|------|------|
| Apache Beam | 2.70.0 | 核心流处理框架 |
| google-auth-library | 1.34.0+ | mTLS 支持所需（CertificateSourceUnavailableException） |
| MySQL Connector/J | 8.0.33 | MySQL 的 JDBC 驱动 |
| Java | 11+ | 运行时要求 |

### 使用的关键特性

- **Streaming Engine**：通过 `--experiments=enable_streaming_engine` 启用，以获得更好的资源利用率
- **Storage Write API**：使用 `STORAGE_API_AT_LEAST_ONCE` 方法进行 CDC 写入
- **有状态处理**：使用 Beam 的 `ValueState` 跟踪最后处理的时间戳
- **带主键的 CDC**：BigQuery 表使用 `PRIMARY KEY (id) NOT ENFORCED` 实现 UPSERT 语义

## 许可证

MIT 许可证 - 可自由使用和修改。
