using System.CommandLine;
using System.CommandLine.Invocation;

namespace LogTapestry.Compactor
{
    internal class Program
    {
        static async Task<int> Main(string[] args)
        {
            var rootCommand = new RootCommand("LogTapestry Compactor Utility");

            var dataOption = new Option<string>(
                "--data",
                description: "Root directory of the Parquet data store")
            { IsRequired = true };

            var compactOlderThanOption = new Option<string>(
                "--compact-older-than",
                () => "1h",
                description: "Do not compact partitions newer than this timespan (e.g., '1h', '2d')");

            rootCommand.AddOption(dataOption);
            rootCommand.AddOption(compactOlderThanOption);

            rootCommand.SetHandler(async (string data, string compactOlderThan) =>
            {
                var compactionTask = new CompactionTask(data, compactOlderThan);
                await compactionTask.RunAsync();
            }, dataOption, compactOlderThanOption);

            return await rootCommand.InvokeAsync(args);
        }
    }
}
