namespace LogTapestry.Core
{
  // File work types enum (moved from DirectoryMonitor)
  public enum FileWorkType
  {
    FileAdded,
    FileChanged,
    FileRemovedOrRotated
  }

  // New data structures for the worker pool model
  public class FileCheckRequest
  {
    public ulong FileId { get; set; }
    public long VolumeSerial { get; set; }
    public string FilePath { get; set; }
    public long LastWriteTimeUtc { get; set; }
    public FileWorkType Type { get; set; }
  }

  public class PositionUpdate
  {
    public ulong FileId { get; set; }
    public long VolumeSerial { get; set; }
    public long Position { get; set; }
    public string FilePath { get; set; }
    public long LastWriteTimeUtc { get; set; }
  }

  public class FileEvent
  {
    public ulong FileId { get; set; }
    public long VolumeSerial { get; set; }
    public string FilePath { get; set; }
    public long LastWriteTimeUtc { get; set; }
    public FileWorkType Type { get; set; }
  }
}
