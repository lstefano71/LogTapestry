## **LogTapestry: Technical Specification - Sprint 2**

**Version:** 1.0
**Date:** August 18, 2025
**Author:** Implementation Team Lead

### **1. Sprint Goal: Robust Ingestion and State Management**

The primary objective of this sprint is to replace the hardcoded, single-file PoC components with a dynamic, robust, and persistent ingestion engine. By the end of this sprint, the ingester will be capable of monitoring a full directory tree, tracking an arbitrary number of log files, surviving restarts, and correctly handling all common file lifecycle events (creation, modification, rotation, deletion).

### **2. Scope and Objectives**

* **Implement the SQLite State Provider:** Create a concrete implementation for persisting and retrieving all ingester state from the `state.sqlite` database.
* **Build the `DirectoryMonitor`:** Implement the full directory scanner and `FileSystemWatcher`-based monitor using NTFS File IDs for reliable file tracking.
* **Build the `TailingManager`:** Implement the component that manages the lifecycle of individual file tailing tasks based on input from the `DirectoryMonitor`.
* **Refactor `TailingTask`:** Enhance the PoC tailing logic to interact with the state provider and handle dynamic rotation signals.
* **Integration Testing:** Create a robust integration test suite that validates the interaction between the file system, the `DirectoryMonitor`, and the `TailingManager`.

### **3. Architectural Changes from PoC**

This sprint involves the following key refactoring efforts:

| PoC Component (Sprint 1) | New Implementation (Sprint 2) | Rationale |
| :--- | :--- | :--- |
| Hardcoded file path | **`DirectoryMonitor`** class | To enable dynamic discovery and monitoring of all log files in a directory. |
| In-memory `long` position | **`SqliteStateProvider`** class | To persist ingestion progress and ensure crash recovery. |
| In-memory `Dictionary` schema | **`SqliteStateProvider`** class | To create a canonical, persistent schema registry. |
| Manual start of one tailer | **`TailingManager`** class | To dynamically manage the lifecycle of hundreds or thousands of concurrent tailing tasks. |

### **4. Detailed Component Specifications**

#### **4.1. State Provider (`LogTapestry.Core`)**

* **Interface:** `IStateProvider`
  * This interface will be defined to abstract away the database, primarily for testing purposes.
* **Class:** `SqliteStateProvider`
  * **Implements:** `IStateProvider`
  * **Constructor:** `public SqliteStateProvider(string databasePath)`
  * **Connection Management:** Will use a single `Microsoft.Data.Sqlite.SqliteConnection` for the lifetime of the object. Methods will create and dispose `SqliteCommand` objects as needed.
  * **Methods and SQL Implementation:**
    * `Task<TrackedFileInfo?> GetTrackedFileAsync(ulong fileId)`:

            ```sql
            SELECT FilePath, Position, LastWriteTimeUtc FROM TrackedFiles WHERE FileId = @fileId AND VolumeSerial = @volumeSerial
            ```

    * `Task UpdateTrackedFileAsync(TrackedFileInfo info)`:

            ```sql
            INSERT INTO TrackedFiles (VolumeSerial, FileId, FilePath, Position, LastWriteTimeUtc) VALUES (...)
            ON CONFLICT(VolumeSerial, FileId) DO UPDATE SET FilePath=excluded.FilePath, Position=excluded.Position, LastWriteTimeUtc=excluded.LastWriteTimeUtc;
            ```

    * `Task RemoveTrackedFileAsync(ulong fileId)`:

            ```sql
            DELETE FROM TrackedFiles WHERE FileId = @fileId AND VolumeSerial = @volumeSerial
            ```

    * `Task<Dictionary<ulong, TrackedFileInfo>> GetAllTrackedFilesAsync()`:

            ```sql
            SELECT FileId, FilePath, Position, LastWriteTimeUtc FROM TrackedFiles
            ```

    * *(Schema Registry methods will be implemented similarly)*

#### **4.2. Directory Monitor (`LogTapestry.Ingester`)**

* **Class:** `DirectoryMonitor`
* **Constructor:** `public DirectoryMonitor(IngesterSettings settings, IStateProvider stateProvider)`
* **Output:** Exposes a `ChannelReader<FileWorkItem>` for the `TailingManager` to consume.
* **Core Method:** `public async Task RunAsync(ChannelWriter<FileWorkItem> writer, CancellationToken token)`
  * This method orchestrates the initial scan and then starts the `FileSystemWatcher`.
* **Private Method:** `private async Task InitialScanAsync()`
    1. Calls `_stateProvider.GetAllTrackedFilesAsync()` to get the last known state.
    2. Uses `Directory.EnumerateFiles` to get all current files on disk.
    3. For each file on disk, calls the `NtfsUtils.GetFileIdentifier` helper.
    4. **Reconciliation Logic:**
        * If a disk file's ID is **not** in the state dictionary: This is a new file. Enqueue a `FileAdded` work item.
        * If a disk file's ID **is** in the state dictionary: Compare `LastWriteTime` from disk vs. state. If newer, enqueue a `FileChanged` work item. Mark the file as "seen" in the state dictionary.
        * After iterating all disk files, any file remaining in the state dictionary that was not "seen" has been deleted. Enqueue a `FileRemovedOrRotated` work item for each.
* **`FileSystemWatcher` Logic:**
  * Initialized with a large `InternalBufferSize` (e.g., 64KB).
  * `OnError` handler will log a critical error and trigger a full `InitialScanAsync()` to resynchronize.
  * Event handlers will be wrapped in a debouncing mechanism to prevent event storms.

#### **4.3. Tailing Manager (`LogTapestry.Ingester`)**

* **Class:** `TailingManager`
* **Constructor:** `public TailingManager(IStateProvider stateProvider)`
* **Internal State:** `private readonly ConcurrentDictionary<ulong, CancellationTokenSource> _activeTailers;`
* **Core Method:** `public async Task RunAsync(ChannelReader<FileWorkItem> workChannel, ChannelWriter<ParsingResult> outputChannel, CancellationToken token)`
    1. Enters an `await foreach` loop on the `workChannel`.
    2. **`FileAdded` / `FileChanged`:**
        * Checks if `_activeTailers.ContainsKey(workItem.FileId)`.
        * If `false`, creates a new `CancellationTokenSource`, adds it to the dictionary, and spawns a new `TailingTask` in the background (`_ = Task.Run(...)`).
    3. **`FileRemovedOrRotated`:**
        * If `_activeTailers.TryRemove(workItem.FileId, out var cts)`, it calls `cts.Cancel()` to gracefully shut down the corresponding `TailingTask`.

#### **4.4. Tailing Task (Refactored Logic)**

* This will be a private, standalone method or class initiated by the `TailingManager`.
* **Initialization:**
    1. Receives a `FileWorkItem` and its `CancellationToken`.
    2. Calls `_stateProvider.GetTrackedFileAsync(fileId)` to get its starting position.
* **Execution Loop:**
    1. Opens the file stream with `FileShare.ReadWrite`.
    2. Seeks to the starting position.
    3. Enters a `while (!token.IsCancellationRequested)` loop.
    4. Performs a read operation. If no new bytes, waits for a short period before retrying.
    5. **Rotation Check:** Before processing the read buffer, it checks the file's current size. If the size is less than the current position, it's a potential rotation. It will then call `NtfsUtils.GetFileIdentifier` on its own path. If the ID has changed, it knows it has been rotated. It will exit its loop, which signals to the `TailingManager` (via task completion) that it has stopped. The `DirectoryMonitor` will have already created a `FileAdded` event for the new file.
    6. Processes the buffer, creates `ParsingResult` objects, and writes them to the `outputChannel`.
    7. After each successful write of a batch to the channel, it calls `_stateProvider.UpdateTrackedFileAsync(...)` to save its new position.

### **5. P/Invoke for NTFS File ID (`LogTapestry.Core`)**

A static helper class will be created to encapsulate the native Windows API call.

```csharp
public static class NtfsUtils
{
    public record FileIdentifier(long VolumeSerial, ulong FileId);

    public static FileIdentifier? GetFileIdentifier(string filePath)
    {
        // P/Invoke structures (BY_HANDLE_FILE_INFORMATION)
        // DllImport for GetFileInformationByHandle
        // Implementation as defined in the architectural document
    }
}
```

### **6. Testing Strategy for Sprint 2**

* **Unit Tests (`LogTapestry.Core.Tests`):**
  * The `SqliteStateProvider` will be tested against an in-memory SQLite database (`DataSource=:memory:`) to verify all CRUD operations and SQL queries are correct.
* **Integration Tests (`LogTapestry.Integration.Tests`):**
  * **Test Case:** `DirectoryMonitor_Should_Correctly_Report_File_Lifecycle`
  * **Setup:**
        1. Create a temporary test directory.
        2. Instantiate an in-memory `SqliteStateProvider` and a `DirectoryMonitor`.
        3. Start `DirectoryMonitor.RunAsync` in the background, reading from its output channel.
  * **Act & Assert:**
        1. Create `file1.log`. Assert a `FileAdded` event is received.
        2. Append text to `file1.log`. Assert a `FileChanged` event is received.
        3. Rename `file1.log` to `file1.log.old`. Assert a `FileRemovedOrRotated` (for the old ID) and `FileAdded` (for the new path but same ID) sequence is handled correctly by the system.
        4. Delete `file1.log.old`. Assert a `FileRemovedOrRotated` event is received.
        5. Create a new `file1.log` (truncate and rewrite). Assert a `FileRemovedOrRotated` (for the old ID) and `FileAdded` (for the new ID) is received.

### **7. Definition of Done**

The sprint is complete when:

1. All hardcoded elements from the Sprint 1 PoC have been removed and replaced with the dynamic components specified above.
2. The `SqliteStateProvider` correctly persists and retrieves all file tracking information across application restarts.
3. The `DirectoryMonitor` can perform an initial scan and correctly reconcile the state with the file system.
4. The `DirectoryMonitor` correctly uses `FileSystemWatcher` and NTFS File IDs to report file creation, renaming, and truncate-style rotation.
5. The `TailingManager` correctly spawns and terminates `TailingTask`s in response to `FileWorkItem`s.
6. The integration test suite for the `DirectoryMonitor` passes successfully.
7. The application can be stopped and restarted, and it will automatically resume tailing all existing files from their last known positions.
