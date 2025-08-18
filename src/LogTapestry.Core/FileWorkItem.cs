
namespace LogTapestry.Core
{
    /// <summary>
    /// Represents a work item for the TailingManager, produced by the DirectoryMonitor.
    /// </summary>
    public enum FileWorkType { FileAdded, FileChanged, FileRemovedOrRotated }

    public record FileWorkItem(string FilePath, FileWorkType WorkType);
}
