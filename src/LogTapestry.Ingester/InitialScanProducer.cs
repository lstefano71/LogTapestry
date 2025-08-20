using LogTapestry.Core;

using Microsoft.Extensions.FileSystemGlobbing;
using Microsoft.Extensions.FileSystemGlobbing.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  /// <summary>
  /// Parallelized initial directory scan producer.
  /// Uses Parallel.ForEachAsync for efficient directory traversal.
  /// </summary>
  public class InitialScanProducer
  {
    private readonly ILogger<InitialScanProducer> _logger;
    private readonly IngesterSettings _settings;
    private readonly IStateProvider _stateProvider;
    private readonly Channel<FileEvent> _outputChannel;
    private readonly Matcher _matcher;

    public InitialScanProducer(
      ILogger<InitialScanProducer> logger,
      IOptions<IngesterSettings> options,
      IStateProvider stateProvider)
    {
      _logger = logger;
      _settings = options.Value;
      _stateProvider = stateProvider;
      _outputChannel = Channel.CreateUnbounded<FileEvent>();

      // Create matcher for include/exclude patterns
      _matcher = new Matcher();
      _matcher.AddIncludePatterns(_settings.IncludePatterns);
      _matcher.AddExcludePatterns(_settings.ExcludePatterns);
    }

    public ChannelReader<FileEvent> Reader => _outputChannel.Reader;

    /// <summary>
    /// Performs parallel initial scan of the directory.
    /// </summary>
    public async Task RunAsync(CancellationToken token)
    {
      try {
        _logger.LogInformation("Starting parallel initial directory scan of {Directory}", _settings.Directory);

        var trackedFiles = await _stateProvider.GetAllTrackedFilesAsync();
        var seen = new HashSet<ulong>();

        var dirRoot = new DirectoryInfo(_settings.Directory);
        var dirWrapper = new DirectoryInfoWrapper(dirRoot);

        // Get all matching files using the globbing matcher
        var matchResult = _matcher.Execute(dirWrapper);

        // Convert to list for parallel processing
        var filePaths = matchResult.Files
          .Select(f => Path.Combine(_settings.Directory, f.Path))
          .ToList();

        _logger.LogInformation("Found {Count} files to scan", filePaths.Count);

        // Process files in parallel using ForEachAsync
        var options = new ParallelOptions {
          MaxDegreeOfParallelism = Environment.ProcessorCount,
          CancellationToken = token
        };

        await Parallel.ForEachAsync(filePaths, options, async (filePath, ct) => {
          try {
            var fileIdObj = NtfsUtils.GetFileIdentifier(filePath);
            if (fileIdObj == null) {
              _logger.LogWarning("Could not get file identifier for {FilePath}", filePath);
              return;
            }

            var id = fileIdObj.FileId;
            seen.Add(id);

            var fileEvent = trackedFiles.TryGetValue(id, out TrackedFileInfo? tracked)
              ? CreateChangedEvent(fileIdObj, filePath)
              : CreateAddedEvent(fileIdObj, filePath);

            await _outputChannel.Writer.WriteAsync(fileEvent, ct);
          } catch (Exception ex) {
            _logger.LogError(ex, "Error processing file {FilePath} during initial scan", filePath);
          }
        });

        // Process removed files
        foreach (var kvp in trackedFiles) {
          if (!seen.Contains(kvp.Key)) {
            var removedEvent = new FileEvent {
              FileId = kvp.Value.FileId,
              VolumeSerial = kvp.Value.VolumeSerial,
              FilePath = kvp.Value.FilePath,
              LastWriteTimeUtc = kvp.Value.LastWriteTimeUtc.Ticks,
              Type = FileWorkType.FileRemovedOrRotated
            };

            await _outputChannel.Writer.WriteAsync(removedEvent, token);
          }
        }

        _logger.LogInformation("Initial scan completed. Processed {Processed} files", filePaths.Count);
      } catch (OperationCanceledException) {
        _logger.LogInformation("Initial scan cancelled");
      } catch (Exception ex) {
        _logger.LogError(ex, "Error during initial directory scan");
      } finally {
        _outputChannel.Writer.Complete();
      }
    }

    private static FileEvent CreateAddedEvent(NtfsUtils.FileIdentifier fileIdObj, string filePath)
    {
      return new FileEvent {
        FileId = fileIdObj.FileId,
        VolumeSerial = fileIdObj.VolumeSerial,
        FilePath = filePath,
        LastWriteTimeUtc = File.GetLastWriteTimeUtc(filePath).Ticks,
        Type = FileWorkType.FileAdded
      };
    }

    private static FileEvent CreateChangedEvent(NtfsUtils.FileIdentifier fileIdObj, string filePath)
    {
      return new FileEvent {
        FileId = fileIdObj.FileId,
        VolumeSerial = fileIdObj.VolumeSerial,
        FilePath = filePath,
        LastWriteTimeUtc = File.GetLastWriteTimeUtc(filePath).Ticks,
        Type = FileWorkType.FileChanged
      };
    }
  }
}
