using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace TweetGate
{
    partial class Program
    {
        static TextWriter  logWriter = Console.Out;
        static Dictionary<string, Func<string[], Task>> actions = new Dictionary<string, Func<string[], Task>>(StringComparer.InvariantCultureIgnoreCase)
        {
            { "saveTweets", SaveTweets},
            { "saveAggregates", SaveAggregates},
            { "printDB", PrintDB},
            { "repl", Repl}
        };

        static async Task Main(string[] args)
        {
            if(args == null || args.Length == 0)
            {
                Help();
                return;
            }

            if(!actions.TryGetValue(args[0], out Func<string[], Task> action))
            {
                Help();
                return;
            }

            await action(args);
            await logWriter.FlushAsync();
        }

        static void Help()
        {
            logWriter.WriteLine("Following actions are supported");
            logWriter.WriteLine(string.Join(Environment.NewLine, actions.Keys));
        }
    }
}
