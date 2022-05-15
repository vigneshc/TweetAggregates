using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using TweetGate.Core;

namespace TweetGate
{
    partial class Program
    {
        // stores tweets to a text file for a given duration.
        static async Task SaveTweets(string[] args)
        {
            if(args.Length != 4)
            {
                logWriter.WriteLine("Usage: TweetGate saveTweets twitterConfig destinationFile durationMinutes");
            }

            string twitterConfigFile = args[1];
            string destinationFile = args[2];
            long durationMinutes = long.Parse(args[3]);
            if( durationMinutes <= 0 )
            {
                throw new InvalidOperationException("Duration should be positive");
            }

            CancellationTokenSource cts = new CancellationTokenSource();
            cts.CancelAfter(TimeSpan.FromMinutes(durationMinutes));

            TwitterConfig twitterConfig = TwitterConfig.FromFile(twitterConfigFile);
            logWriter.WriteLine($"Using config from: '{twitterConfigFile} Minutes to save: '{durationMinutes}' Writing to: {destinationFile}");
            logWriter.WriteLine($"Track Terms: {twitterConfig.TrackTerms}");
            Stream twitterStream = await TwitterStream.GetStream(twitterConfig);
            long count = 0;

            using(var destination = File.OpenWrite(destinationFile))
            using(var writer = new StreamWriter(destination))
            {
                Stopwatch stopWatch = Stopwatch.StartNew();
                foreach(var tweet in TwitterStream.GetTweetsEnumerable(twitterStream))
                {
                    count++;
                    writer.WriteLine(tweet);
                    if(stopWatch.Elapsed.TotalMinutes > durationMinutes)
                    {
                        break;
                    }
                }
            }

            logWriter.WriteLine($"Wrote Tweet Count: {count}");
        }
    }
}
