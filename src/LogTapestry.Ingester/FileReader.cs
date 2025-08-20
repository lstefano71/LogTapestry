using LogTapestry.Core;

using Microsoft.Extensions.Logging;

using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  /// <summary>
  /// Static utility class for core file reading logic.
  /// Replaces the file handling in TailingManager with temporary file access.
  /// </summary>
  public static class FileReader
  {
    /// <summary>
    /// Reads and parses a file from the given position, then immediately closes it.
    /// This prevents file handle exhaustion in the worker pool model.
    /// </summary>
    public static async Task ReadAndParseFileAsync(
      FileCheckRequest request,
      ChannelWriter<PositionUpdate> outputChannel,
      ILogger logger,
      CancellationToken token)
    {
      try {
        // Get the plugin for this file
        var plugin = GetPluginForFile(request.FilePath);
        if (plugin == null) {
          logger.LogWarning("No plugin matched for file: {FilePath}", request.FilePath);
          return;
        }

        // Get current tracked position
        var trackedPosition = await GetTrackedPositionAsync(request);
        var position = trackedPosition ?? 0;

        // Open file temporarily, read new content, then close immediately
        using var fs = new FileStream(request.FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        if (position > fs.Length) {
          // File was truncated, reset position
          position = 0;
        }

        fs.Seek(position, SeekOrigin.Begin);

        var parser = new RegexLogParser(plugin, request.FilePath);
        var newPosition = position;
        var buffer = new List<string>();

        using (var sr = new StreamReader(fs, leaveOpen: true)) {
          string? line;
          while ((line = await sr.ReadLineAsync(token)) != null) {
            buffer.Add(line);
          }
        }

        // Parse the new content
        var results = parser.Parse([.. buffer]);

        // Send position update if we read new content
        if (buffer.Count > 0) {
          newPosition = fs.Position;

          var positionUpdate = new PositionUpdate {
            FileId = request.FileId,
            VolumeSerial = request.VolumeSerial,
            Position = newPosition,
            FilePath = request.FilePath,
            LastWriteTimeUtc = request.LastWriteTimeUtc
          };

          await outputChannel.WriteAsync(positionUpdate, token);
          logger.LogDebug("Updated position for {FilePath}: {Position}", request.FilePath, newPosition);
        }

        // Log parsing results
        foreach (var result in results) {
          if (result.IsSuccess && result.Entry != null) {
            // In the new architecture, this would be sent to a parsing pipeline
            logger.LogDebug("Parsed log entry from {FilePath}", request.FilePath);
          } else {
            logger.LogWarning("Log parsing failed: {ErrorMessage}. Source: {Source}",
              result.ErrorMessage, result.Source);
          }
        }
      } catch (Exception ex) {
        logger.LogError(ex, "Error reading file {FilePath}", request.FilePath);
      }
    }

    /// <summary>
    /// Checks if a file has been rotated by comparing file IDs.
    /// </summary>
    public static bool IsFileRotated(string filePath, ulong originalFileId)
    {
      var currentId = NtfsUtils.GetFileIdentifier(filePath);
      return currentId == null || currentId.FileId != originalFileId;
    }

    /// <summary>
    /// Gets the last write time for a file without keeping it open.
    /// </summary>
    public static long GetLastWriteTimeUtc(string filePath)
    {
      return File.GetLastWriteTimeUtc(filePath).Ticks;
    }

    private static PluginSettings? GetPluginForFile(string filePath)
    {
      // This would need access to the plugin settings
      // For now, return null - this will be implemented when we refactor IngesterService
      return null;
    }

    private static async Task<long?> GetTrackedPositionAsync(FileCheckRequest request)
    {
      // This would need access to the state provider
      // For now, return null - this will be implemented when we refactor IngesterService
      return null;
    }
  }
}
