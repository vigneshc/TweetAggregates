using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Buffers;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.StreamProcessing;

namespace TweetGate.Core
{
    // class for reading from twitter stream.
    // Uses https://developer.twitter.com/en/docs/twitter-api/v1/tweets/filter-realtime/api-reference/post-statuses-filter 
    public static class TwitterStream
    {
        static readonly TimeSpan LogFrequency = TimeSpan.FromMinutes(5);
        static readonly HttpClient client = new HttpClient();
        public static IEnumerable<string> GetTweetsEnumerable(Stream twitterStream)
        {
            using(var sr = new StreamReader(twitterStream))
            {
                string line = sr.ReadLine();
                while(!string.IsNullOrEmpty(line))
                {
                    yield return line;
                    line = sr.ReadLine();
                }
            }
        }

        // Uses System.IO.Pipelines for continuously pushing tweets and blocking if data hasn't been processed (for back pressure).
        // https://docs.microsoft.com/en-us/dotnet/standard/io/pipelines
        public static async Task StartTwitterPump(
            Func<Task<Stream>> getTwitterStreamAsync, 
            PipeWriter pipewriter, 
            TextWriter logWriter, 
            bool isDirectRead,
            CancellationToken token)
        {
            const int BufferSizeInBytes = 512 * 1024;
            logWriter.WriteLine($"Started Reading.");
            Subject<int> bytesProcessed = new Subject<int>();

            // Emit processed kbs every LogFrequency to LogWriter.
            var logTask = bytesProcessed
            .Select(b => StreamEvent.CreatePoint(DateTime.UtcNow.Ticks, b/1024.0))
            .ToStreamable(DisorderPolicy.Drop(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time((ulong)TimeSpan.FromSeconds(1).Ticks))
            .TumblingWindowLifetime(LogFrequency.Ticks)
            .Sum(e => e)
            .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
            .Where(e => e.IsData)
            .ForEachAsync(l => 
            {
                logWriter.WriteLine($"{new DateTime(l.StartTime, DateTimeKind.Utc):o} Read kb in last {LogFrequency} : {l.Payload}");
                logWriter.Flush();
            });

            
            Stopwatch streamReadTimer = Stopwatch.StartNew();
            var twitterStream = await getTwitterStreamAsync();
            
            // repeatedly read tweets from twitter stream and write to pipeWriter.
            while(!token.IsCancellationRequested)
            {
                Memory<byte> memory = pipewriter.GetMemory(BufferSizeInBytes);
                int bytesRead = 0;
                try
                {
                    bytesRead = await twitterStream.ReadAsync(memory, token);
                }
                catch(Exception e)
                {
                    logWriter.WriteLine($"{DateTime.UtcNow:o} TwitterStream ReadAsync failed. Exception : {e}");
                    if(streamReadTimer.Elapsed < TimeSpan.FromMinutes(1))
                    {
                        // avoid retries failures are too often and exit instead.
                        logWriter.WriteLine($"{DateTime.UtcNow:o} Not restarting because read duration was less: {streamReadTimer.Elapsed}");
                        bytesProcessed.Dispose();
                        await pipewriter.CompleteAsync();
                        throw;
                    }

                    // recreate connection to twitter stream on failure.
                    logWriter.WriteLine($"{DateTime.UtcNow:o} Recreating TwitterStream");
                    twitterStream = await getTwitterStreamAsync();
                    streamReadTimer.Restart();
                    continue;
                }

                if(bytesRead == 0)
                {
                    if(isDirectRead)
                    {
                        // recreate connection to potentially fix zero byte reads.
                        if(streamReadTimer.Elapsed < TimeSpan.FromMinutes(1))
                        {
                            logWriter.WriteLine($"{DateTime.UtcNow:o} Read Zero Bytes. Not restarting because read duration was less: {streamReadTimer.Elapsed} . Exiting");
                        }
                        else
                        {
                            twitterStream.Dispose();
                            logWriter.WriteLine($"{DateTime.UtcNow:o} Recreating TwitterStream");
                            twitterStream = await getTwitterStreamAsync();
                        }
                    }
                    else
                    {
                        logWriter.WriteLine($"{DateTime.UtcNow:o} NotDirectRead. Read zero bytes, exiting read");
                    }
                }
                else
                {
                    // update bytes processed for logging.
                    bytesProcessed.OnNext(bytesRead);
                    pipewriter.Advance(bytesRead);

                    FlushResult flushResult = await pipewriter.FlushAsync(token);
                    if(flushResult.IsCompleted || flushResult.IsCanceled)
                    {
                        logWriter.WriteLine("Writer exited. Disposing read stream");
                        twitterStream.Dispose();
                        break;
                    }
                }
            }

            logWriter.WriteLine($"Completed reading. IsCancellationRequested : {token.IsCancellationRequested}");
            bytesProcessed.OnCompleted();
            bytesProcessed.Dispose();

            pipewriter.Complete();
        }

        // Read side of the pipe.
        // Continously read tweets from pipe, and push to the tweetStringSubject.
        public static async Task ProcessTweetStream(PipeReader reader, Subject<string> tweetStringSubject, TextWriter logWriter, CancellationToken token)
        {
            byte newLine = (byte)'\n';
            logWriter.WriteLine("Started processing");

            while(!token.IsCancellationRequested)
            {
                ReadResult readResult = await reader.ReadAsync(token);

                ReadOnlySequence<byte> readBuffer = readResult.Buffer;
                SequencePosition? newLinePosition;

                do
                {
                    newLinePosition = readBuffer.PositionOf(newLine);

                    // read a line.
                    if(newLinePosition != null)
                    {
                        ReadOnlySequence<byte> lineBytes = readBuffer.Slice(0, newLinePosition.Value);
                        string line = null;

                        if(lineBytes.IsSingleSegment)
                        {
                            line = Encoding.UTF8.GetString(lineBytes.First.Span);
                        }
                        else
                        {
                            StringBuilder sb = new StringBuilder();
                            foreach(ReadOnlyMemory<byte> slice in lineBytes)
                            {
                                sb.Append(Encoding.UTF8.GetString(slice.Span));
                            }

                            line = sb.ToString();
                        }

                        if(!string.IsNullOrWhiteSpace(line))
                        {
                            // send to tweet Subject for processing.
                            tweetStringSubject.OnNext(line);
                        }

                        readBuffer = readBuffer.Slice(readBuffer.GetPosition(1, newLinePosition.Value));
                    }
                }while(newLinePosition != null);

                
                reader.AdvanceTo(readBuffer.Start, readBuffer.End);
                if(readResult.IsCanceled || readResult.IsCompleted)
                {
                    break;
                }
            }

            logWriter.WriteLine($"Completed processing. IsCancellationRequested : {token.IsCancellationRequested}");

            reader.Complete();
            tweetStringSubject.OnCompleted();
        }

        // create a connection to the twitter stream.
        // https://developer.twitter.com/en/docs/twitter-api/v1/tweets/filter-realtime/api-reference/post-statuses-filter 
        public static async Task<Stream> GetStream(TwitterConfig config)
        {
            var oauth_version = "1.0";
            var oauth_signature_method = "HMAC-SHA1";

            // unique request details
            var oauth_nonce = Convert.ToBase64String(new ASCIIEncoding().GetBytes(DateTime.Now.Ticks.ToString()));
            var oauth_timestamp = Convert.ToInt64(
                (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc))
                    .TotalSeconds).ToString();

            const string StreamsUrl = "https://stream.twitter.com/1.1/statuses/filter.json";
            string language = "en";
            if(!string.IsNullOrEmpty(config.Languages))
            {
                language = Uri.EscapeDataString(config.Languages);
            }

            // create oauth signature
            var baseString = 
                $"language={language}&oauth_consumer_key={config.OAuthConsumerKey}&oauth_nonce={oauth_nonce}&oauth_signature_method={oauth_signature_method}&" +
                $"oauth_timestamp={oauth_timestamp}&oauth_token={config.OAuthToken}&oauth_version={oauth_version}&track={Uri.EscapeDataString(config.TrackTerms)}";
            
            baseString = string.Concat("POST&", Uri.EscapeDataString(StreamsUrl), "&", Uri.EscapeDataString(baseString));

            var compositeKey = string.Concat(Uri.EscapeDataString(config.OAuthConsumerSecret),
            "&", Uri.EscapeDataString(config.OAuthTokenSecret));

            string oauth_signature;
            using (var hasher = new HMACSHA1(ASCIIEncoding.ASCII.GetBytes(compositeKey)))
            {
                oauth_signature = Convert.ToBase64String(
                hasher.ComputeHash(ASCIIEncoding.ASCII.GetBytes(baseString)));
            }

            // create the request header
            var authHeader =
                $"OAuth oauth_nonce=\"{Uri.EscapeDataString(oauth_nonce)}\", oauth_signature_method=\"{Uri.EscapeDataString(oauth_signature_method)}\", " +
                $"oauth_timestamp=\"{Uri.EscapeDataString(oauth_timestamp)}\", oauth_consumer_key=\"{Uri.EscapeDataString(config.OAuthConsumerKey)}\", " +
                $"oauth_token=\"{Uri.EscapeDataString(config.OAuthToken)}\", oauth_signature=\"{Uri.EscapeDataString(oauth_signature)}\", " +
                $"oauth_version=\"{Uri.EscapeDataString(oauth_version)}\"";

            // make the request
            ServicePointManager.Expect100Continue = false;

            string resource_url = $"{StreamsUrl}?language={language}&track={Uri.EscapeDataString(config.TrackTerms)}" ;
            client.Timeout = Timeout.InfiniteTimeSpan;
            var streamApiRequest = new HttpRequestMessage(HttpMethod.Post, resource_url);
            streamApiRequest.Headers.Add("Authorization", authHeader);
            streamApiRequest.Headers
                .Accept
                .Add(new MediaTypeWithQualityHeaderValue("application/x-www-form-urlencoded"));

            var tresponse = await client.SendAsync(streamApiRequest, HttpCompletionOption.ResponseHeadersRead);
            return await tresponse.Content.ReadAsStreamAsync();
        }
    }
}