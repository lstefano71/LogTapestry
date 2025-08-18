using System;
using System.Threading.Tasks;

namespace LogTapestry.Compactor
{
    public class CompactionTask
    {
        private readonly string _dataPath;
        private readonly string _compactOlderThan;

        public CompactionTask(string dataPath, string compactOlderThan)
        {
            _dataPath = dataPath;
            _compactOlderThan = compactOlderThan;
        }

        public async Task RunAsync()
        {
            Console.WriteLine($"Compaction started for data path: {_dataPath}, compacting partitions older than: {_compactOlderThan}");

            var candidates = FindCompactionCandidates();
            foreach (var partitionPath in candidates)
            {
                Console.WriteLine($"Compaction candidate: {partitionPath}");
                // TODO: Call ExecuteCompaction(partitionPath)
                // await ExecuteCompaction(partitionPath);
            }

            await Task.CompletedTask;
        }

        public IEnumerable<string> FindCompactionCandidates()
        {
            var candidates = new List<string>();
            var threshold = ParseTimeSpan(_compactOlderThan);

            foreach (var dir in Directory.GetDirectories(_dataPath, "*", SearchOption.AllDirectories))
            {
                var dirInfo = new DirectoryInfo(dir);

                // Check age
                if (DateTime.UtcNow - dirInfo.LastWriteTimeUtc < threshold)
                    continue;

                // Must contain landing/
                var landingPath = Path.Combine(dir, "landing");
                if (!Directory.Exists(landingPath))
                    continue;

                // Must NOT contain _COMPACTION_COMPLETE
                var markerPath = Path.Combine(dir, "_COMPACTION_COMPLETE");
                if (File.Exists(markerPath))
                    continue;

                candidates.Add(dir);
            }

            return candidates;
        }

        public async Task ExecuteCompaction(string partitionPath)
        {
            // 1. Read all Parquet files in landing/ using DuckDB
            // 2. Rewrite data into DataColumn arrays for Parquet.Net
            // 3. Write new large Parquet files with .tmp extension
            // 4. Delete landing/ directory
            // 5. Rename .tmp files to .parquet
            // 6. Create _COMPACTION_COMPLETE marker file
            // 7. Handle errors and cleanup temp files if needed

            // TODO: Implement compaction logic
            await Task.CompletedTask;
        }

        private TimeSpan ParseTimeSpan(string input)
        {
            // Simple parser: "1h", "2d", "30m"
            if (input.EndsWith("h"))
                return TimeSpan.FromHours(double.Parse(input.TrimEnd('h')));
            if (input.EndsWith("d"))
                return TimeSpan.FromDays(double.Parse(input.TrimEnd('d')));
            if (input.EndsWith("m"))
                return TimeSpan.FromMinutes(double.Parse(input.TrimEnd('m')));
            throw new ArgumentException("Invalid timespan format. Use '1h', '2d', or '30m'.");
        }
    }
}
