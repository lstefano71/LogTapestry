// LogTapestry.Core/IStateProvider.cs
namespace LogTapestry.Core
{
  public interface IStateProvider
  {
    Task<TrackedFileInfo?> GetTrackedFileAsync(ulong fileId, long volumeSerial);
    Task UpdateTrackedFileAsync(TrackedFileInfo info);
    Task RemoveTrackedFileAsync(ulong fileId, long volumeSerial);
    Task<Dictionary<ulong, TrackedFileInfo>> GetAllTrackedFilesAsync();
    // Schema registry methods can be added here as needed
  }

  public class TrackedFileInfo
  {
    public long VolumeSerial { get; set; }
    public ulong FileId { get; set; }
    public string FilePath { get; set; }
    public long Position { get; set; }
    public System.DateTime LastWriteTimeUtc { get; set; }
  }
}
