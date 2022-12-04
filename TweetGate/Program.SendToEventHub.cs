using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;

using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

using TweetGate.Core;

namespace TweetGate
{
    partial class Program
    {
        // sends tweets to an eventhub for a given duration.
        static async Task SendToEventHub(string[] args)
        {
            if(args.Length != 4)
            {
                logWriter.WriteLine("Usage: TweetGate saveTweets twitterConfig eventhubConfig durationMinutes");
            }

            string twitterConfigFile = args[1];
            string eventhubConfigFile = args[2];
            long durationMinutes = long.Parse(args[3]);

            CancellationTokenSource cts = new CancellationTokenSource();
            cts.CancelAfter(TimeSpan.FromMinutes(durationMinutes).Add(TimeSpan.FromSeconds(10)));

            TwitterConfig twitterConfig = TwitterConfig.FromFile(twitterConfigFile);
            logWriter.WriteLine($"Using config from: '{twitterConfigFile} Minutes to save: '{durationMinutes}' Writing to Eventhub defined in: {eventhubConfigFile}");
            logWriter.WriteLine($"Track Terms: {twitterConfig.TrackTerms}");
            
            // create eventhub client.
            var eventhubConfig = EventhubConfig.Parse(eventhubConfigFile);
            var producerOptions = new EventHubBufferedProducerClientOptions()
            {
                EnableIdempotentRetries = true,
                MaximumWaitTime = TimeSpan.FromSeconds(eventhubConfig.ProducerMaximumWaitTimeInSeconds),
                RetryOptions = new EventHubsRetryOptions()
                {
                    MaximumRetries = 10
                }
            };

            ulong tweetCount = 0;
            ulong eventDataCount = 0;
            ulong failureCount = 0;
            var eventhubProducer = new EventHubBufferedProducerClient(eventhubConfig.ConnectionString, eventhubConfig.EventhubName, producerOptions);
            eventhubProducer.SendEventBatchFailedAsync +=   args => {
                Interlocked.Increment(ref failureCount);
                logWriter.WriteLine($"Failed to write. Continuous FailureCount = {Interlocked.Read(ref failureCount)} Exception: {args.Exception.Message}");
                return Task.CompletedTask;
            };

            eventhubProducer.SendEventBatchSucceededAsync += args => {
                Interlocked.Increment(ref eventDataCount);
                Interlocked.Exchange(ref failureCount, 0);
                return Task.CompletedTask;
            };
            var eventhubProperties = await eventhubProducer.GetEventHubPropertiesAsync(cts.Token);

            Stopwatch stopWatch = Stopwatch.StartNew();
            Stopwatch bufferedTime = Stopwatch.StartNew();
            MemoryStream ms = null;
            Stream wrappedStream = null;
            StreamWriter sw = null;
            long maxEventhubMessageSizeInBytes = Math.Max(1, eventhubConfig.MaxEventhubMessageSizeInBytes);
            var maxBufferTime = TimeSpan.FromSeconds(eventhubConfig.MaxTweetAccumulationTimeInSeconds);
            
            var traceTask = Observable
                .Interval(TimeSpan.FromMinutes(1))
                .ForEachAsync(i => logWriter.WriteLine($"{DateTime.UtcNow:o} Wrote TweetCount: {Interlocked.Read(ref tweetCount)} EventData Count: {Interlocked.Read(ref eventDataCount)}"));

            // send buffered tweets to eventhub.
            int nextPartitionId = 0;
            async Task SendBufferedEventAsync()
            {
                if(sw == null)
                {
                    return;
                }

                sw.Flush();
                wrappedStream.Flush();
                ms.Flush();

                if(ms.Position > 0)
                {
                    ms.Seek(0, SeekOrigin.Begin);
                    var eventData = new EventData(BinaryData.FromStream(ms));
                    var options = new EnqueueEventOptions()
                    {
                        PartitionId = eventhubProperties.PartitionIds[nextPartitionId]
                    };

                    await eventhubProducer.EnqueueEventAsync(eventData, options, cts.Token);
                    nextPartitionId = (nextPartitionId + 1) % eventhubProperties.PartitionIds.Length;
                    bufferedTime.Restart();
                }

                ms = null;
                wrappedStream = null;
                sw = null;
            }

            Func<Task<Stream>> getTwitterStream = () => TwitterStream.GetStream(twitterConfig);

            // create tasks for reading from twitter stream and sending tweets tweetSubject.
            var pipe = new Pipe();
            Subject<string> tweetStrings = new Subject<string>();
            Task twitterStreamReadTask = TwitterStream.StartTwitterPump(getTwitterStream, pipe.Writer, logWriter, isDirectRead: true, token: cts.Token)
                .ContinueWith(a1 => Console.WriteLine("Read Task exited"));
            Task twitterStreamProcessTask = TwitterStream.ProcessTweetStream(pipe.Reader, tweetStrings, logWriter, cts.Token)
                .ContinueWith(a1 => "Process task exited");

            // subscription to send tweets to eventhub.
            Task writeToEventHubTask = tweetStrings.ForEachAsync(async tweet => 
            {
                tweetCount++;
                if(ms == null)
                {
                    ms = new MemoryStream();
                    wrappedStream = eventhubConfig.GzipCompressed ? new GZipStream(ms, CompressionLevel.SmallestSize) : ms;
                    sw = new StreamWriter(wrappedStream);
                }

                sw.WriteLine(tweet);

                // kick off send when buffer limit is reached.
                if(ms.Position >= maxEventhubMessageSizeInBytes || bufferedTime.Elapsed > maxBufferTime)
                {
                    await SendBufferedEventAsync();
                }
            },
            cts.Token)
            .ContinueWith(a1 => Console.WriteLine("Send to eventhub exited"));

            await Task.WhenAny(twitterStreamReadTask, twitterStreamProcessTask, writeToEventHubTask);
            await SendBufferedEventAsync();
            await eventhubProducer.FlushAsync(cts.Token);
            await eventhubProducer.CloseAsync();

            logWriter.WriteLine($"Wrote TweetCount: {Interlocked.Read(ref tweetCount)} EventData Count: {Interlocked.Read(ref eventDataCount)}");
        }
    }
}
