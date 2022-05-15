using System;
using System.IO;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using System.Reactive.Linq;
using System.Reactive.Subjects;

using TweetGate.Core;
using TweetGate.Core.Serialization;
using Microsoft.StreamProcessing;
using Newtonsoft.Json;

namespace TweetGate
{
    partial class Program
    {
        // reads twitter stream and stores aggregates in a rocks db.
        static async Task SaveAggregates(string[] args)
        {
            if(args.Length != 4)
            {
                logWriter.WriteLine("Usage: TweetGate saveAggregates <mode=file/direct> <inputFileName/configFileName> outputDBPath");
            }

            Func<Task<Stream>> getTwitterStreamAsync = null;
            bool isDirectRead = false;
            switch(args[1].ToLower())
            {
                // read from a local file.
                case "file":
                    logWriter.WriteLine($"Reading from file: {args[2]}");
                    getTwitterStreamAsync = () => {
                        var twitterStream = File.OpenRead(args[2]);
                        twitterStream.Seek(0, SeekOrigin.Begin);
                        return Task.FromResult((Stream)twitterStream);
                    };

                    break;
                
                // read from twitter stream directly.
                case "direct":
                    logWriter.WriteLine($"Reading directly using config file: {args[2]}");
                    isDirectRead = true;
                    TwitterConfig twitterConfig = TwitterConfig.FromFile(args[2]);
                    var logFile = Path.Combine(twitterConfig.LogsDirectory ?? string.Empty, Path.GetRandomFileName());
                    logWriter.WriteLine($"Log file: {logFile}");
                    logWriter = new StreamWriter(logFile);
                    getTwitterStreamAsync = () => TwitterStream.GetStream(twitterConfig);
                    break;
                default:
                    logWriter.WriteLine($"Unknown mode {args[1]}");
                    return;
            }

            CancellationTokenSource cts = new CancellationTokenSource();
            await ProcessTweetsAndSaveAggregates(getTwitterStreamAsync, isDirectRead, args[3], cts.Token);
        }

        // kick off twitter stream read, process tweets and write results to rocks DB.
        static async Task ProcessTweetsAndSaveAggregates(Func<Task<Stream>> getTwitterStream, bool isDirectRead, string rocksDBPath, CancellationToken token)
        {
            CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(token);
            Subject<string> tweetStrings = new Subject<string>();
            var pipe = new Pipe();
            
            // start twitter read. This will push data pipe.
            Task readTask = TwitterStream.StartTwitterPump(getTwitterStream, pipe.Writer, logWriter, isDirectRead, cts.Token);
            logWriter.WriteLine($"Started read task");

            // observable that creates timestamped tweet events.
            var tweets = tweetStrings
            .Select(l => JsonConvert.DeserializeObject<RawTweet>(l))
            .Where(tweet => tweet!= null && tweet.GetTimestamp() > 0)
            .Select(t => StreamEvent.CreatePoint(t.GetTimestamp(), t))
            .Publish();

            // observable that produces aggregates per time window from tweet events.
            var aggregateObservables = Query.SimpleAggregate(tweets);
            logWriter.WriteLine("Created aggregate observable");

            // initialize rocks DB store.
            var store = new RocksDBStore(rocksDBPath, isReadOnly: false, logAction: logWriter.WriteLine);
            store.Initialize();

            logWriter.WriteLine("Initialized DB");

            // start task that repeatedly writes available aggregates.
            Task storeTask = store.PersistObservableAsync(aggregateObservables, cts.Token);
            tweets.Connect();
            logWriter.WriteLine("Connected tweets observable");
            logWriter.WriteLine("Started store persist subscription");

            // start the task that reads from pipe and pushes it to tweet events stream.
            Task writeTask = TwitterStream.ProcessTweetStream(pipe.Reader, tweetStrings, logWriter, cts.Token);
            logWriter.WriteLine("Started tweet string producer task");

            // wait on reader and writer tasks.
            await Task.WhenAny(readTask, writeTask, storeTask);
            logWriter.WriteLine($"ReadTask IsCompleted: {readTask.IsCompleted} . Exception : {readTask.Exception}");
            logWriter.WriteLine($"WriteTask IsCompleted: {writeTask.IsCompleted} . Exception : {writeTask.Exception}");
            logWriter.WriteLine($"StoreTask IsCompleted: {storeTask.IsCompleted} . Exception : {storeTask.Exception}");
            cts.Cancel();
            
            await Task.WhenAll(readTask, writeTask, storeTask);
            logWriter.WriteLine($"ReadTask, WriteTask and StoreTask completed");
        }
    }
}
