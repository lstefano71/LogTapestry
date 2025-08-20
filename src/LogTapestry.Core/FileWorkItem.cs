
namespace LogTapestry.Core
{
  /// <summary>
  /// Represents a work item for the TailingManager, produced by the DirectoryMonitor.
  /// NOTE: This class is deprecated and will be removed in the worker pool refactoring.
  /// </summary>
  public record FileWorkItem(string FilePath, FileWorkType WorkType);
}
