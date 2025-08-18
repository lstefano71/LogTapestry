## **LogTapestry: Technical Specification - Sprint 5**

**Version:** 1.1
**Date:** August 18, 2025
**Author:** Implementation Team Lead

### **1. Sprint Goal: Hardening, Testing, and Enterprise Readiness**

The primary objective of this sprint is to ensure the LogTapestry suite is robust, reliable, and ready for deployment within our enterprise environment. This involves expanding test coverage to cover failure scenarios, performing manual "chaos testing," creating a standardized build process using **Cake**, and producing comprehensive documentation for both end-users and system operators.

### **2. Scope and Objectives**

*   **Expand Test Coverage:** Increase unit and integration test coverage, specifically targeting edge cases and error handling paths.
*   **Failure and Recovery Testing:** Manually perform a series of tests to ensure the system behaves predictably under adverse conditions.
*   **Build Automation with Cake:** Create a `build.cake` script to automate the entire build, test, and packaging process.
*   **Write User Documentation:** Create a clear guide for using the `logtapestry-query.exe` tool.
*   **Write Operator Documentation:** Create a comprehensive guide for installing, configuring, and monitoring the `LogTapestry.Ingester` and `LogTapestry.Compactor`.
*   **Final Code Review and Cleanup:** Perform a full code review of the entire solution to address any remaining tech debt, inconsistencies, or performance hotspots.

### **3. Detailed Specifications**

#### **3.1. Expanded Test Coverage (`LogTapestry.Integration.Tests`)**

The focus is on "unhappy path" testing. New test cases will be added to simulate failures.

*   **Test Case:** `DirectoryMonitor_Should_Recover_From_Watcher_Buffer_Overflow`
    *   **Goal:** Verify the `OnError` resynchronization logic.
    *   **Implementation:** This is difficult to trigger programmatically. The test will use a mock `FileSystemWatcher` (via an interface abstraction) where a method `mockWatcher.TriggerErrorEvent()` can be called.
    *   **Act:** Start the monitor, create a file, and then trigger the mock error event. After the event, create a second file.
    *   **Assert:** Assert that the monitor successfully detects the second file after its resynchronization scan, proving it recovered.

*   **Test Case:** `DataSink_Should_Retry_And_Eventually_Fail_On_Persistent_IO_Error`
    *   **Goal:** Verify the retry and error logging logic in the `DataSink`.
    *   **Implementation:** The `DataSink`'s dependency on the file system will be abstracted behind an interface (e.g., `IParquetWriter`). The mock implementation will be configured to throw an `IOException` a specific number of times before succeeding (or failing permanently).
    *   **Act:** Call `DataSink.WriteBatchAsync` with the mock writer configured to fail 3 times.
    *   **Assert:** Assert that the logger recorded 3 `Warning` or `Debug` messages for the retries and a final `Error` message for the ultimate failure.

*   **Test Case:** `IngesterService_Should_Shutdown_Gracefully_When_Pipeline_Is_Full`
    *   **Goal:** Ensure backpressure doesn't cause a deadlock during shutdown.
    *   **Setup:** Configure the central channel with a very small capacity (e.g., 10). Inject a mock `DataSink` that has a long `Task.Delay` in its `WriteBatchAsync` method, ensuring it consumes slowly.
    *   **Act:** Start the ingester, flood it with log lines to fill the channel, and then trigger a graceful shutdown.
    *   **Assert:** Assert that the `StopAsync` method completes successfully within a reasonable timeout and that all log entries written before the shutdown signal were persisted.

#### **3.2. Manual Failure and Recovery Testing Plan**

This is a checklist for manual testing in a staging environment.

| Scenario | Action | Expected Outcome |
| :--- | :--- | :--- |
| **Ingester Crash** | Run the ingester, generate logs, then abruptly kill the process (`taskkill /f`). | On restart, the ingester logs that it is performing an initial scan and successfully resumes processing from the exact point it left off with no data loss. |
| **Disk Full** | Fill the data disk to 99.9% capacity while the ingester is running. | The `DataSink` logs multiple `Error` messages about write failures. The system remains stable. Once space is cleared, ingestion resumes successfully on the next batch. |
| **Log Directory Vanishes** | While the ingester is running, rename or delete the monitored log directory. | The `DirectoryMonitor` logs a `Critical` error. The `/health` endpoint reports `Unhealthy`. When the directory is restored, the monitor recovers on its next rescan and resumes normal operation. |
| **Corrupt State DB** | Stop the ingester, write garbage data into the `state.sqlite` file, then restart. | The ingester fails to start, logging a `Critical` error about a corrupt or unreadable state database. It should not enter a crash loop. |
| **Compactor Interruption** | Kill the `logtapestry-compactor.exe` process mid-compaction. | The data directory is left in a clean state (the `landing/` directory is untouched, and no partial `.tmp` files remain). Rerunning the compactor completes the job successfully. |

#### **3.3. Build and Deployment Automation (Cake)**

A `build.cake` script will be created at the root of the repository. It will be bootstrapped with standard PowerShell/bash scripts for easy execution.

*   **Cake Script (`build.cake`):**
    *   **Global Variables:** Define paths for solution file, release directory, configuration (`Release`), and runtime identifier (`win-x64`).
    *   **Targets:**
        *   **`Clean`:** Deletes all `bin`, `obj`, and the main `/release` directory.
        *   **`Restore`:** Runs `DotNetRestore` on the solution.
        *   **`Build`:** Runs `DotNetBuild` on the solution in `Release` configuration.
        *   **`Test`:** Runs `DotNetTest` on the solution, ensuring all tests pass before packaging.
        *   **`Publish`:** This will be the main target. It depends on `Clean`, `Restore`, `Build`, and `Test`.
            *   It will execute `DotNetPublish` three times, once for each executable (`Ingester`, `Compactor`, `Query`).
            *   The settings for `DotNetPublish` will include:
                *   `SelfContained = true`
                *   `Runtime = "win-x64"`
                *   `PublishSingleFile = true`
                *   `Configuration = "Release"`
            *   The output of each publish command will be directed to the `/release` directory.
        *   **`Package`:** This target depends on `Publish`. It will create a single `.zip` archive of the `/release` directory, including a sample `appsettings.json` and the batch scripts for service installation. This zip file is the final deployment artifact.
    *   **Default Target:** The default target will be `Test`.

*   **Deployment Scripts:**
    *   The `release/` directory will contain the three published executables.
    *   It will also include helper batch scripts:
        *   `install-service.bat`: A script that uses `sc.exe` (Service Control) to create the Windows Service for the ingester.
        *   `uninstall-service.bat`: A script to remove the service.
        *   `run-compactor-manual.bat`: A simple wrapper to run the compactor with default settings.

#### **3.4. Documentation**

Two key Markdown documents will be created in a `/docs` directory and included in the final packaged zip file.

*   **`OperatorGuide.md`:**
    *   **Installation:** Step-by-step guide on how to unzip the package and run `install-service.bat`. How to schedule the compactor using Windows Task Scheduler.
    *   **Configuration:** A detailed breakdown of every setting in `appsettings.json`, with examples and recommendations for "high-throughput" vs. "good-citizen" modes.
    *   **Monitoring:** How to use the `/health` and `/metrics` endpoints. A guide to interpreting the structured logs, especially common warnings and errors.
    *   **Troubleshooting:** A guide for diagnosing common problems (e.g., "Why is my data not appearing?").
*   **`UserGuide.md`:**
    *   **Querying Basics:** A simple, non-technical guide to using `logtapestry-query.exe`.
    *   **Schema Discovery:** How to find out what fields are available to query.
    *   **Examples:** A library of common query examples (e.g., "Find the top 10 errors from yesterday," "Count logins by user").
    *   **Output Formats:** How to use the `--output` flag to get data in JSON or CSV for use in other tools.
    *   **Advanced Queries:** A brief introduction to using `UNNEST` for complex queries, with examples.

#### **3.5. Final Code Review**

*   **Process:** The entire team will participate in a formal code review of the `main` branch.
*   **Checklist:**
    *   **Consistency:** Are naming conventions, async/await patterns, and logging styles consistent across the codebase?
    *   **Clarity:** Is the code easy to understand? Are there enough comments for complex sections?
    *   **Error Handling:** Is every I/O operation and external call wrapped in appropriate error handling?
    *   **Resource Management:** Are all `IDisposable` objects correctly disposed (e.g., streams, database connections)?
    *   **Hardcoded Values:** Are all configurable values (paths, timeouts, buffer sizes) driven by the settings file?

### **6. Definition of Done**

The sprint—and the project's initial development phase—is complete when:
1.  All new "unhappy path" integration tests pass successfully.
2.  All scenarios in the manual failure testing plan have been executed and their outcomes verified.
3.  The `build.cake` script can be run to produce a clean `LogTapestry-v1.0.0.zip` release package.
4.  The `OperatorGuide.md` and `UserGuide.md` are complete and have been reviewed for clarity and accuracy.
5.  The final code review is complete, and all high-priority issues identified have been resolved.
6.  A final end-to-end demonstration, showcasing ingestion, monitoring, compaction, and querying using the packaged release artifact, is successfully presented to the project stakeholders.