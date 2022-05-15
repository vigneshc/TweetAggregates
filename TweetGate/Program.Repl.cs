using System;
using System.IO;
using System.Threading.Tasks;
using System.Linq;

using TweetGate.Core;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace TweetGate
{
    partial class Program
    {
        // Provides repl mode read for the db.
        static async Task Repl(string[] args)
        {
            if(args.Length < 2)
            {
                logWriter.WriteLine("Usage: TweetGate Repl outputDBPath <optionalOutputDirectory>");
                return;
            }

            var store = new RocksDBStore(args[1], isReadOnly: true, logAction: logWriter.WriteLine);
            store.Initialize();
            bool shouldExit = false;
            string outputDirectory = string.Empty;

            if(args.Length == 3)
            {
                outputDirectory = args[2];
                Directory.CreateDirectory(outputDirectory);
            }

            while(!shouldExit)
            {
                Console.Write(">");
                var command = Console.ReadLine();
                if(string.IsNullOrWhiteSpace(command))
                {
                    continue;
                }
                var commandParts = command.Split(' ');
                string output = string.Empty;
                switch(commandParts[0].ToLowerInvariant())
                {
                    case "exit":
                        shouldExit = true;
                        break;
                    case "getsummary":
                        output = JsonConvert.SerializeObject(store.GetSummary(), Formatting.Indented);
                        break;
                    case "getcounts":
                        var timeRange = GetTimeRange(commandParts);
                        output = JsonConvert.SerializeObject(
                            store.GetCounts(timeRange.StartTime, timeRange.EndTime).ToArray(), 
                            Formatting.Indented);
                        break;
                    case "gettopmentionsstring":
                        timeRange = GetTimeRange(commandParts);
                        var optionalScreenName = commandParts.Length == 4 ? commandParts[3]: null;
                        output = JsonConvert.SerializeObject(
                            store.GetTopMentionsString(timeRange.StartTime, timeRange.EndTime, optionalScreenName)
                                .Select(e => JObject.Parse(e))
                                .ToArray(), 
                            Formatting.Indented);
                        break;
                    case "gettophashtagsstring":
                        timeRange = GetTimeRange(commandParts);
                        var optionalHashTag = commandParts.Length == 4 ? commandParts[3]: null;
                        output = JsonConvert.SerializeObject(
                            store.GetTopHashTagsString(timeRange.StartTime, timeRange.EndTime, optionalHashTag)
                                .Select(e => JObject.Parse(e))
                                .ToArray(), 
                            Formatting.Indented);
                        break;
                    case "gettopretweetsstring":
                        timeRange = GetTimeRange(commandParts);
                        output = JsonConvert.SerializeObject(
                            store.GetTopRetweetsString(timeRange.StartTime, timeRange.EndTime)
                                .Select(e => JsonConvert.DeserializeObject<TopRetweets[]>(e))
                                .ToArray(), 
                            Formatting.Indented);
                        break;
                    case "getrecenttopmentionsstring":
                        int recentCount = int.Parse(commandParts[1]);
                        output = JsonConvert.SerializeObject(
                            store.GetRecentTopMentionsString(recentCount)
                                .Select(e => JObject.Parse(e))
                                .ToArray(), 
                            Formatting.Indented);
                        break;
                    case "getrecenttophashtagsstring":
                        recentCount = int.Parse(commandParts[1]);
                        output = JsonConvert.SerializeObject(
                            store.GetRecentTopHashTagsString(recentCount)
                                .Select(e => JObject.Parse(e))
                                .ToArray(), 
                            Formatting.Indented);
                        break;
                    case "getrecenttopretweetsstring":
                        recentCount = int.Parse(commandParts[1]);
                        output = JsonConvert.SerializeObject(
                            store.GetRecentTopRetweetsString(recentCount)
                                .Select(e => JsonConvert.DeserializeObject<TopRetweets[]>(e))
                                .ToArray(), 
                            Formatting.Indented);
                        break;
                }

                if(!string.IsNullOrEmpty(output))
                {
                    if(!string.IsNullOrEmpty(outputDirectory))
                    {
                        var fileName = Path.Combine(outputDirectory, $"{commandParts[0]}.{DateTime.UtcNow.Ticks}.json");
                        await File.WriteAllTextAsync(fileName, output);
                    }
                    else
                    {
                        Console.WriteLine("___________________________________________");
                        Console.WriteLine(output);
                        Console.WriteLine("___________________________________________");
                    }
                }
            }
        }

        static (DateTime StartTime, DateTime EndTime) GetTimeRange(string[] args)
        {
            var startTime = DateTime.Parse(args[1]).ToUniversalTime();
            var endTime = DateTime.Parse(args[2]).ToUniversalTime();
            return (startTime, endTime);
        }
    }
}
