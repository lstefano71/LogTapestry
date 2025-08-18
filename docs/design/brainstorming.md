# Ingester

## Context

Sofia is an old school application where everything is implemented from scratch. The Database is a collection of files in various different formats, all in a directory and subdirectories. The same directory is used for the configuration files, the logs, and the data files.
New logs are born almost every day and they have very different structures, names, conventions for the names and even extensions. We are interested in the logs in text format.
In the future we hope to transition to a central log collector for structured logs but for now we need to implement a solution that can handle the current situation.
The idea is to create an ingester which in real time tails all the logs, including the new ones as they are added to the directory, wherever they may be in the directory tree.
The ingested entries should be committed to a database, so that they can be queried later.
The database should be able to deal with entries which are not structured, so that we can handle the current situation where the logs are not structured and have different formats. But a structure albeit primitive is provided by the ingestion, those fields should be used to efficiently query the logs later.
It should be possible to query the database even as the ingester is running.
I am leaning towards using parquet files accompanied by a sqlite database to store the state of the ingester, and the metadata that describes the parquet files. 

## Requirements

- The ingester should be able to handle new log files as they are created in the directory
- The ingester should be able to handle different log formats and structures
- The ingester should be able to handle large log files
- The ingester should be able to handle logs in text format

## Plugins

- The ingester should support plugins to handle different log formats and structures
- Plugins should be able to be added or removed without restarting the ingester
- Plugins should be able to specify how to parse the log files and extract the relevant information
- The plugins should themselves be able to be configured to specify how to parse the log files. A set of prebuilt plugins should be versatile enough to cover most of the log files.

In particular, plugins should be at the very minimum able to parse the timestamp of the log entry and turn it into a UTC timestamp.
Each log entry will be parsed into a structured format with the following fields:

- `timestamp`: The UTC timestamp of the log entry
- `level`: The log level (e.g., INFO, ERROR, DEBUG)
- `message`: The log message
- `source`: The source of the log entry (e.g., file name, line number)

The built-in plugins should be able to be configured with a list of regexes to extract fields. The fields will be extracted from the log entry and added to the structured format. Extracting fields should be optional, and the plugin should be able to handle log entries that do not match the regexes. Extracting fields will build a template that will be included in the structured format.

### Example

Assuming a property configured plugin, from the entry:

```
2023-10-01 12:00:00 INFO [myapp] User logged in: user_id=12345, session_id=abcde
```

in file `logs/dbver.log`, the plugin could extract:

The plugin could extract:

- `timestamp`: 2023-10-01T12:00:00Z
- `level`: INFO
- `message`: User logged in: user_id=12345, session_id=abcde
- `source`: logs/dbver.log
- `fields`: { "user_id": "12345", "session_id": "abcde" }
- `template`: "User logged in: user_id={user_id}, session_id={session_id}"

## Configuration

The ingester should be configurable to specify:

- the directory to watch for new log files
- a list of glob patterns to match the log files
- a list of glob patterns to exclude certain log files

## How to tail log files

The ingester should use a file system watcher to monitor the specified directory for new log files. When a new log file is created, the ingester should start tailing the file in real time. The ingester should also be able to handle existing log files that are already present in the directory when it starts.
In general the ingester should read the log files line by line and parse each log entry using the configured. The built-in plugin should have a multiline mode capable of entries which span multiple lines.

During the execution the ingester should keep track of the latest processed log entry for each file, so that it can resume processing from the last entry in case of a restart or crash.
This can be done by maintaining a state file that stores the last processed entry for each file. The state file should probably be a sqlite database to mimic what the tail plugin of fluent bit does.
The ingester should also be able to handle log rotation, where a log file is renamed and a new log file is created. The ingester should be able to detect the rotation and continue processing the new log file from the last processed entry.

### Postponing processing of log entries

The ingester should be able to postpone the processing of log entries that are not yet complete, such as multiline entries that span multiple lines but not limited to those. As we know, I/O does not guarantee that even single lines will be written in full at the end of each write operation, so the ingester should be able to handle partial lines and wait for the next write operation to complete the line.

## Configuration of the default plugin
The configuration file should be simple for humans. Abominations like YAML should be avoided. A simple JSON or INI format should be used instead.
The global glob patterns specified a the level of the ingester will control the list of interesting files.
Below those, a pair include/exclude lists of glob patterns will identify a subset of files to be processed by a certain instance of a plugin. So, for example:
```json
{
  "ingester": {
    "directory": "/var/log/sofia",
    "include_patterns": ["*.log", "*.txt"],
    "exclude_patterns": ["*.gz", "*.zip"]
  },
  "plugins": [
    {
      "name": "default",
      "include_patterns": ["*.log"],
      "exclude_patterns": ["*.debug.log"],
      "config": {
        "timestamp_regex": "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}",
        "utc_timestamp": true,
        "level_regex": "(INFO|ERROR|DEBUG)",
        "message_regex": "(.*)",
        "fields_regexes": [
          {"regex": "user_id=(\\d+)", "field_name": "user_id"},
          {"regex": "session_id=([a-z0-9]+)", "field_name": "session_id"}
        ]
      }
    },
    {
      "name": "default",
      "include_patterns": ["*.txt"],
      "exclude_patterns": ["*.debug.txt"],
      "config": {
        "timestamp_regex": "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}",
        "level_regex": "(INFO|ERROR|DEBUG)",
        "utc_timestamp": false,
        "message_regex": "(.*)",
        "fields_regexes": [
          {"regex": "user_id=(\\d+)", "field_name": "user_id"},
          {"regex": "session_id=([a-z0-9]+)", "field_name": "session_id"}
        ]
      }
  ]
}
```

## Error Handling
The ingester should handle errors gracefully. If a log file cannot be read or parsed, the ingester should log the error and continue processing other log files. The ingester should also be able to handle malformed log entries and skip them without crashing.
The ingester should log errors in a separate error log file, which can be configured in the ingester configuration. The error log file should include the timestamp, log level, and a message describing the error.

## Performance Considerations
The ingester should be able to handle large log files efficiently. It should use buffered I/O to read log files in chunks and process them in parallel if possible. The ingester should also be able to handle a large number of log files without consuming too much memory or CPU resources.
The ingester should be able to handle high log volumes without dropping log entries. It should be able to process log entries in real time and keep up with the rate of incoming log entries.
