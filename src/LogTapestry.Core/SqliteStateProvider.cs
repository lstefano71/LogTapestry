// LogTapestry.Core/SqliteStateProvider.cs
using Microsoft.Data.Sqlite;

namespace LogTapestry.Core
{
  public class SqliteStateProvider : IStateProvider, IDisposable
  {
    private readonly SqliteConnection _connection;

    public SqliteStateProvider(string databasePath)
    {
      _connection = new SqliteConnection($"Data Source={databasePath}");
      _connection.Open();
      EnsureSchema();
    }

    private void EnsureSchema()
    {
      var cmd = _connection.CreateCommand();
      cmd.CommandText = @"
                CREATE TABLE IF NOT EXISTS TrackedFiles (
                    VolumeSerial INTEGER NOT NULL,
                    FileId INTEGER NOT NULL,
                    FilePath TEXT NOT NULL,
                    Position INTEGER NOT NULL,
                    LastWriteTimeUtc INTEGER NOT NULL,
                    PRIMARY KEY (VolumeSerial, FileId)
                );
            ";
      cmd.ExecuteNonQuery();
    }

    public async Task<TrackedFileInfo?> GetTrackedFileAsync(ulong fileId, long volumeSerial)
    {
      var cmd = _connection.CreateCommand();
      cmd.CommandText = @"
                SELECT FilePath, Position, LastWriteTimeUtc
                FROM TrackedFiles
                WHERE FileId = @fileId AND VolumeSerial = @volumeSerial
            ";
      cmd.Parameters.AddWithValue("@fileId", (long)fileId);
      cmd.Parameters.AddWithValue("@volumeSerial", volumeSerial);

      using var reader = await cmd.ExecuteReaderAsync();
      if (await reader.ReadAsync()) {
        return new TrackedFileInfo {
          VolumeSerial = volumeSerial,
          FileId = fileId,
          FilePath = reader.GetString(0),
          Position = reader.GetInt64(1),
          LastWriteTimeUtc = DateTimeOffset.FromUnixTimeSeconds(reader.GetInt64(2)).UtcDateTime
        };
      }
      return null;
    }

    public async Task UpdateTrackedFileAsync(TrackedFileInfo info)
    {
      var cmd = _connection.CreateCommand();
      cmd.CommandText = @"
                INSERT INTO TrackedFiles (VolumeSerial, FileId, FilePath, Position, LastWriteTimeUtc)
                VALUES (@volumeSerial, @fileId, @filePath, @position, @lastWriteTimeUtc)
                ON CONFLICT(VolumeSerial, FileId) DO UPDATE SET
                    FilePath=excluded.FilePath,
                    Position=excluded.Position,
                    LastWriteTimeUtc=excluded.LastWriteTimeUtc;
            ";
      cmd.Parameters.AddWithValue("@volumeSerial", info.VolumeSerial);
      cmd.Parameters.AddWithValue("@fileId", (long)info.FileId);
      cmd.Parameters.AddWithValue("@filePath", info.FilePath);
      cmd.Parameters.AddWithValue("@position", info.Position);
      cmd.Parameters.AddWithValue("@lastWriteTimeUtc", new DateTimeOffset(info.LastWriteTimeUtc).ToUnixTimeSeconds());

      await cmd.ExecuteNonQueryAsync();
    }

    public async Task RemoveTrackedFileAsync(ulong fileId, long volumeSerial)
    {
      var cmd = _connection.CreateCommand();
      cmd.CommandText = @"
                DELETE FROM TrackedFiles
                WHERE FileId = @fileId AND VolumeSerial = @volumeSerial
            ";
      cmd.Parameters.AddWithValue("@fileId", (long)fileId);
      cmd.Parameters.AddWithValue("@volumeSerial", volumeSerial);

      await cmd.ExecuteNonQueryAsync();
    }

    public async Task<Dictionary<ulong, TrackedFileInfo>> GetAllTrackedFilesAsync()
    {
      var cmd = _connection.CreateCommand();
      cmd.CommandText = @"
                SELECT VolumeSerial, FileId, FilePath, Position, LastWriteTimeUtc
                FROM TrackedFiles
            ";

      var result = new Dictionary<ulong, TrackedFileInfo>();
      using var reader = await cmd.ExecuteReaderAsync();
      while (await reader.ReadAsync()) {
        var info = new TrackedFileInfo {
          VolumeSerial = reader.GetInt64(0),
          FileId = (ulong)reader.GetInt64(1),
          FilePath = reader.GetString(2),
          Position = reader.GetInt64(3),
          LastWriteTimeUtc = DateTimeOffset.FromUnixTimeSeconds(reader.GetInt64(4)).UtcDateTime
        };
        result[info.FileId] = info;
      }
      return result;
    }

    public void Dispose()
    {
      _connection?.Dispose();
    }
  }
}
