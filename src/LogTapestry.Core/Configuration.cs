using System;
using System.Collections.Generic;

namespace LogTapestry.Core
{
    public class LogTapestrySettings
    {
        public IngesterSettings Ingester { get; set; } = new();
        public List<PluginSettings> Plugins { get; set; } = new();
    }

    public class IngesterSettings
    {
        public string Directory { get; set; } = "";
        public List<string> IncludePatterns { get; set; } = new();
        public int PipelineBufferCapacity { get; set; } = 10000;
        public int MaxParsingParallelism { get; set; } = Environment.ProcessorCount;
    }

    public class PluginSettings
    {
        public string Type { get; set; } = "";
        public string Name { get; set; } = "";
        public List<string> IncludePatterns { get; set; } = new();
        public RegexPluginConfig Config { get; set; } = new();
    }

    public class RegexPluginConfig
    {
        public string StartOfEntryRegex { get; set; } = "";
        public string TimestampRegex { get; set; } = "";
        public string LevelRegex { get; set; } = "";
        public List<FieldRegex> FieldsRegexes { get; set; } = new();
    }

    public class FieldRegex
    {
        public string Regex { get; set; } = "";
        public string FieldName { get; set; } = "";
        public string Type { get; set; } = "string"; // "long", "double", etc.
    }
}