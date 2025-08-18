# **Product Requirements Document: LogTapestry v1.0**

**Author:** Gemini AI
**Date:** August 18, 2025
**Status:** Final Draft

### 1. Introduction

#### 1.1. Problem Statement

The "Sofia" application is a critical legacy system that generates logs in a decentralized and unstructured manner. Log files are spread across a directory tree in various text-based formats, with inconsistent naming conventions, structures, and timestamps. There is no central system for log aggregation or analysis. Troubleshooting production issues is a slow, manual process involving `grep` and other command-line tools across numerous files, making it difficult to get a timely, holistic view of the system's behavior.

#### 1.2. Proposed Solution

**LogTapestry** is a high-performance, self-contained log ingestion and query engine designed to solve this problem. It will run as a discreet background service on the Sofia server, providing three core capabilities:

1. **Ingestion:** It will continuously monitor the specified log directory, tailing all relevant text-based log files in real-time, including new files as they are created.
2. **Structuring & Storage:** It will parse the unstructured log lines into a structured format, intelligently typing extracted data fields. This structured data will be stored in a highly optimized, columnar format (Apache Parquet) on the local disk.
3. **Querying:** It will provide a powerful command-line tool that allows operators to run SQL queries against the entire corpus of ingested logs, enabling fast, sophisticated analysis of both real-time and historical data.

LogTapestry is designed to be a "good citizen," operating with a minimal and configurable performance footprint to avoid impacting the primary Sofia application.

### 2. Goals and Objectives

* **Reliable Data Capture:** Ensure every relevant log line from the target directory is captured with an "at-least-once" delivery guarantee, even in the event of an application crash or restart.
* **Enable High-Performance Queries:** Transform the log data into a format that allows for analytical queries to complete in seconds, not minutes, dramatically reducing troubleshooting time.
* **Minimize Operational Footprint:** Operate discreetly in the background with configurable limits on CPU, I/O, and memory usage.
* **Provide Operational Insight:** Offer clear, structured logs and health check endpoints to make LogTapestry itself a transparent and monitorable component of the system.
* **Self-Contained & Portable:** The system (ingester, compactor, query tool) should be deployable as self-contained executables with minimal dependencies, facilitating both production deployment and offline analysis in a lab environment.

### 3. User Personas

* **Sofia, the System Administrator / SRE:** Sofia is responsible for the health and uptime of the production system. Her primary need is to rapidly diagnose and resolve issues. She needs to query logs from the last hour or day to find error patterns or trace a specific user's activity. She values reliability, low performance overhead, and powerful, fast query capabilities.
* **Devin, the Developer:** Devin is tasked with debugging complex, non-reproducible bugs found in production. He receives a snapshot of production log data (a copy of the LogTapestry data directory) to analyze in a lab environment. He needs to run deep, complex analytical queries over large historical datasets without impacting the live system.

### 4. Core Components & Features

#### 4.1. The Ingester Service (`logtapestryd`)

This is the long-running background service responsible for data collection and processing.

* **F-1.1: File Discovery and Monitoring**
  * The service shall monitor a specified root directory and all its subdirectories for log files.
  * Configuration shall support include/exclude glob patterns (e.g., `*.log`, `!*.gz`) to define the set of target files.
  * The service shall perform a full scan on startup to process files modified while it was offline.
  * The service shall use real-time filesystem notifications to discover new files and changes to existing files.
  * **F-1.1.1: Robust Log Rotation Handling:** The service must reliably handle log rotation scenarios. On Windows (NTFS), it shall use the filesystem's File ID to track files through renames. On other platforms (or as a fallback), it shall use file content signatures to detect truncate-and-rewrite rotations.

* **F-1.2: State Management & Resiliency**
  * The service shall use a local SQLite database to persist its state.
  * For each tracked file, the state shall include the last successfully processed byte offset.
  * Upon restart, the service shall use this state to resume processing from where it left off, guaranteeing no data is lost.

* **F-1.3: Pluggable Parsing Pipeline**
  * The ingester shall support a plugin architecture for parsing different log formats. For v1.0, a single, highly configurable "regex" plugin will be provided.
  * **F-1.3.1: Default Regex Plugin Configuration:** The plugin shall be configurable with:
    * A regex to identify the start of a log entry (for multi-line handling).
    * Regexes to extract the primary `timestamp`, `level`, and `message` fields.
    * A list of named capture-group regexes to extract dynamic key-value fields (e.g., `user_id`, `session_id`).
    * **Type information** for each extracted field (e.g., `long`, `double`, `string`). If a value cannot be parsed to its configured type, it shall be stored as a string in a fallback collection.

* **F-1.4: Schema Registry**
  * The SQLite database shall contain a `FieldSchema` table that acts as a central registry for all dynamically discovered fields and their canonical types.
  * The type of a field is determined by the first plugin that successfully parses it. Subsequent data for that field must conform to the registered type or be placed in the fallback collection.

* **F-1.5: Data Sink & Storage Format**
  * Successfully parsed log entries shall be written to local disk in the **Apache Parquet** format.
  * **F-1.5.1: Columnar Schema:** The Parquet files shall use a specific schema designed for query performance, with separate map columns for each data type (`fields_string`, `fields_long`, etc.). This structure is an internal detail, abstracted away from the user by the query tool.
  * **F-1.5.2: Hive Partitioning:** Data shall be organized on disk using Hive-style partitioning by date (e.g., `/data/year=2025/month=08/day=18/...`).
  * **F-1.5.3: Two-Tier Storage:** To balance low-latency ingestion with high-performance querying, the ingester shall write to a two-tier storage system:
    * **Landing Zone:** A directory where small, frequent batches of data are written to ensure data is queryable within seconds.
    * **Optimized Zone:** A directory where a separate Compactor tool will later merge the small files into large, query-optimized files.

#### 4.2. The Compactor Tool (`logtapestry-compactor`)

This is a standalone command-line tool that performs data maintenance.

* **F-2.1: Data Compaction**
  * The tool shall scan the data directory for time partitions (e.g., a specific hour) that contain small files in the `landing` zone.
  * It shall read all small files for a given time window and rewrite them as one or more large, optimized Parquet files in the `Optimized` zone.
  * This process is designed to solve the "small file problem," drastically improving query performance on historical data.

* **F-2.2: Idempotent & Offline Operation**
  * After successfully compacting a directory, the tool shall write a `_COMPACTION_COMPLETE` marker file to prevent reprocessing.
  * The tool shall be runnable as a scheduled task (e.g., hourly) on a live system.
  * It must also be runnable offline on a copied data snapshot in a lab environment to prepare it for analysis.

#### 4.3. The Query Tool (`logtapestry-query`)

This is the primary user interface for accessing the data, delivered as a standalone command-line tool.

* **F-3.1: SQL-based Querying**
  * The tool shall provide a powerful SQL interface for querying the log data. This will be powered by an embedded DuckDB engine.
  * The tool will be able to query across the entire dataset, seamlessly combining data from both the "Landing" and "Optimized" zones.

* **F-3.2: Schema Abstraction**
  * The tool shall hide the physical complexity of the Parquet storage from the user.
  * A user will write a query using logical field names, e.g., `SELECT * WHERE user_id = 12345`.
  * The tool will use the `FieldSchema` table from the SQLite database to automatically rewrite this query to target the correct physical column, e.g., `... WHERE fields_long['user_id'] = 12345`.

* **F-3.3: Dual-Mode Operation**
  * **Online Mode:** The tool can be pointed at a live system's data and state directories to query near real-time data.
  * **Offline Mode:** The tool can be pointed at a copied snapshot of the data and state directories for forensic analysis.

* **F-3.4: User Experience**
  * The tool shall support multiple output formats, including a human-readable table (default), JSON, and CSV.
  * The tool shall have an interactive mode (REPL) for running multiple queries in a single session.

### 5. Non-Functional Requirements (NFRs)

* **NFR-1: Performance & Resource Usage**
  * **Ingestion Latency:** Log entries should be persisted and available for query within 10 seconds of being written to the source file.
  * **Resource Throttling ("Good Citizen Mode"):** The ingester and compactor must be configurable to limit their impact on the host system. This includes:
    * Configurable `MaxDegreeOfParallelism` for the CPU-intensive parsing stage.
    * Configurable I/O throttling or scheduling for the compactor to ensure it runs during off-peak hours.
  * **File Access:** The ingester must open files for reading using `FileShare.ReadWrite` to avoid locking conflicts with the Sofia application.

* **NFR-2: Reliability & Error Handling**
  * The ingester service must not crash due to malformed log lines or transient I/O errors on a single file.
  * All errors (I/O, parsing, configuration) shall be logged to a dedicated, structured (JSON-formatted) error log file.
  * Parsing failures on individual log entries shall be logged as warnings, and the ingester shall discard the malformed entry and continue processing.

* **NFR-3: Operability**
  * **Health Checks:** The ingester service shall expose a simple HTTP `/health` endpoint for external monitoring.
  * **Metrics:** The service shall expose key operational metrics (e.g., entries processed, files tailed) via a `/metrics` endpoint in the Prometheus exposition format.
  * **Deployment:** The application suite shall be publishable as self-contained executables and include sample configuration for running as a Windows Service or via `systemd` on Linux.

* **NFR-4: Usability**
  * **Configuration Validation:** The ingester shall have a `--validate` command-line flag that performs a "dry run" check of the configuration file for syntax errors and logical inconsistencies without starting the service.

### 6. Out of Scope for v1.0

* Real-time loading/unloading of plugins while the service is running.
* A graphical user interface (GUI) for querying or configuration.
* Advanced schema evolution (e.g., changing the type of a field after data has been written).
* Authentication, authorization, or remote access to the query endpoint.
* Ingestion from sources other than the local filesystem (e.g., network sockets, message queues).

### 7. Success Metrics

* **Mean Time to Resolution (MTTR):** A measurable reduction in the time it takes for the operations team to diagnose production issues related to the Sofia application.
* **Adoption:** The tool is successfully deployed to the production environment and is actively used by the SysAdmin/SRE team as the primary method for log analysis.
* **System Stability:** The LogTapestry ingester achieves >99.9% uptime and causes zero performance-related incidents on the host Sofia application over a 3-month period.
* **Query Performance:** 95% of queries over a 24-hour time range complete in under 5 seconds.

