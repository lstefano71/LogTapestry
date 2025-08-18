using System.CommandLine;

namespace LogTapestry.Compactor
{
  internal class Program
  {
    static int Main(string[] args)
    {
      var dataOption = new Option<string>("--data") { Arity = ArgumentArity.ExactlyOne };
      dataOption.Description = "Root directory of the Parquet data store";

      var compactOlderThanOption = new Option<string>("--compact-older-than") { Arity = ArgumentArity.ExactlyOne };
      compactOlderThanOption.Description = "Do not compact partitions newer than this timespan (e.g., '1h', '2d')";

      var rootCommand = new RootCommand("LogTapestry Compactor Utility")
      {
                dataOption,
                compactOlderThanOption
            };

      rootCommand.SetAction(async (ParseResult parseResult) => {
        var data = parseResult.GetValue(dataOption);
        var compactOlderThan = parseResult.GetValue(compactOlderThanOption);
        if (string.IsNullOrEmpty(compactOlderThan)) compactOlderThan = "1h";
        var compactionTask = new CompactionTask(data, compactOlderThan);
        await compactionTask.RunAsync();
      });

      return rootCommand.Parse(args).Invoke();
    }
  }
}
