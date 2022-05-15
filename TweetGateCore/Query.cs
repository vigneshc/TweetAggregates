using System;
using System.Reactive.Linq;
using System.Linq;
using Microsoft.StreamProcessing;
using TweetGate.Core.Serialization;

namespace TweetGate.Core
{
    // Provides queries for tweet aggregates.
    public static class Query
    {
        static readonly long WindowSize = TimeSpan.FromMinutes(10).Ticks;
        static readonly long HopSize = TimeSpan.FromMinutes(1).Ticks;
        static readonly long DisOrderTicks = TimeSpan.FromSeconds(5).Ticks;

        static readonly int NumberOfExamples = 10;
        static readonly int NumberOfAggregatesPerWindow = 10;

        // Creates queries for tweet counts, top retweets, top mentions and top hashtags.
        // https://github.com/microsoft/Trill
        public static AggregateObservables SimpleAggregate(IObservable<StreamEvent<RawTweet>> input)
        {
            var result = new AggregateObservables();

            // project RawTweet and only keep relevant fields.
            // create two streams from input for downstream consumption.
            var inputEvents = input
            .ToStreamable(
                DisorderPolicy.Adjust(Query.DisOrderTicks),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time((ulong)TimeSpan.FromSeconds(1).Ticks))
            .Select(r => new { 
                Text = GetText(r),
                ScreenName = r.user.screen_name,
                HashTags = r.entities.hashtags.Select(h => h.text).ToArray(),
                Mentions = r.entities.user_mentions.Select(u => u.screen_name).ToArray(),
                OriginalTweetId = r.retweeted_status != null ? r.retweeted_status.id : -1,
                Id = r.id,
                FollowerCount = r.user.followers_count,
                Time = r.GetTimestamp()
            })
            .Multicast(2);

            // compute counts every tumbling window.
            result.TumblingCounts = inputEvents[0]
            .TumblingWindowLifetime(WindowSize)
            .Count()
            .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
            .Where(e => e.IsData)
            .Select(e => new Counts() { WindowTime = e.EndTime, Count = e.Payload});

            // Group by hopping window and create four substreams.
            var inputs = inputEvents[1]
            .HoppingWindowLifetime(WindowSize, HopSize)
            .Multicast(4);

            // Select top screen names mentioned in each hopping window.
            var mentionsAggregate = inputs[0]
            .Where(t => t.Mentions.Length > 0)
            .SelectMany(t => t.Mentions.Select(m => new { ScreenName = m, FollowerCount = t.FollowerCount, Text = t.Text, Id = t.Id, OriginalTweetId = t.OriginalTweetId}))
            .GroupAggregate(
                t => t.ScreenName, // aggregate per screen name.
                g => g.Count(),
                g => g.Sum(t => t.FollowerCount),
                g => g.TopK(t => t.FollowerCount, NumberOfExamples), // example tweets, from handles with most follower count.
                (k, count, followerCountSum, top) => new { ScreenName = k.Key, FollowerCountSum = followerCountSum, TweetCount = count, TopTweets = top })
            .Select(e => new TopMentions()
            {
                FollowerCountSum = e.FollowerCountSum,
                TweetCount = e.TweetCount,
                ScreenName = e.ScreenName,
                TopTweets = e.TopTweets
                    .Select(t => t.Payload)
                    .Select(p => new TweetDescription()
                    {
                        Id = p.Id,
                        FollowerCount = p.FollowerCount,
                        Text = p.Text,
                        ScreenName = p.ScreenName,
                        OriginalTweetId = p.OriginalTweetId,
                    })
                    .GroupBy(p => p.OriginalTweetId == -1 ? p.Id : p.OriginalTweetId) // dedupe and select one tweet per retweeted tweet.
                    .Select(g => g.OrderByDescending(e => e.FollowerCount).First())
                    .ToArray()
            });

            // select the top few screen names from all mentions aggregate.
            result.TopMentions = GetTopItems(mentionsAggregate);
            
            // select top hash tags mentioned in each hopping window.
            var hashtagsAggregate = inputs[1]
            .Where(t => t.HashTags.Length > 0)
            .SelectMany(t => t.HashTags.Select(h => new { ScreenName = t.ScreenName, HashTag = h, FollowerCount = t.FollowerCount, Text = t.Text, Id = t.Id, t.OriginalTweetId }))
            .GroupAggregate(
                t => t.HashTag, // aggregate per hash tag
                g => g.Count(),
                g => g.Sum(t => t.FollowerCount),
                g => g.TopK(t => t.FollowerCount, NumberOfExamples), // example tweets, from handles with most follower count
                (k, count, followerCountSum, top) => new { HashTag = k.Key, FollowerCountSum = followerCountSum, TweetCount = count, TopTweets = top })
            .Select(e => new TopHashTags()
            {
                FollowerCountSum = e.FollowerCountSum,
                TweetCount = e.TweetCount,
                HashTag = e.HashTag,
                TopTweets = e.TopTweets
                    .Select(t => t.Payload)
                    .Select(p => new TweetDescription()
                    {
                        Id = p.Id,
                        FollowerCount = p.FollowerCount,
                        Text = p.Text,
                        ScreenName = p.ScreenName,
                        OriginalTweetId = p.OriginalTweetId,
                    })
                    .GroupBy(p => p.OriginalTweetId == -1 ? p.Id : p.OriginalTweetId) // dedupe and select one tweet per retweeted tweet.
                    .Select(g => g.OrderByDescending(e => e.FollowerCount).First())
                    .ToArray()
            });
            result.TopHashtags = GetTopItems(hashtagsAggregate);

            // select top retweets.
            var retweetsAggregate = inputs[2]
            .Where(t => t.OriginalTweetId != -1)
            .Select(t => new { Id = t.OriginalTweetId, Text = t.Text, ScreenName = t.ScreenName, FollowerCount = t.FollowerCount})
            .GroupAggregate(
                t => t.Id,
                g => g.Max(t => t.Text),
                g => g.Count(),
                g => g.Sum(t => t.FollowerCount),
                g => g.TopK(t => t.FollowerCount, NumberOfExamples),
                (k, text, count, followerCountSum, top) => new { Id = k.Key, Text = text, FollowerCountSum = followerCountSum, Count = count, TopTweets = top})
            .Select(e => new TopRetweets()
            {
                FollowerCountSum = e.FollowerCountSum,
                TweetCount = e.Count,
                Id = e.Id,
                TopUsers = e.TopTweets.Select(t => t.Payload.ScreenName).ToArray(),
                Text = e.Text
            });
            result.TopRetweets = GetTopItems(retweetsAggregate);
            
            result.HoppingCounts = inputs[3]
            .Count()
            .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
            .Where(e => e.IsData)
            .Select(e => new Counts() { WindowTime = e.EndTime, Count = e.Payload});

            return result;
        }

        // select top items for a given stream every tumbling window.
        private static IObservable<T[]> GetTopItems<T>(IStreamable<Empty, T> items) where T: TopItems
        {
            return items
            .TumblingWindowLifetime(HopSize)
            .Aggregate(e => e.TopK(t => t.FollowerCountSum, NumberOfAggregatesPerWindow))
            .Select(e => e.Select(t=> t.Payload).ToArray())
            .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
            .Where(e => e.IsData)
            .Select(e => 
            {
                foreach(var p in e.Payload)
                {
                    p.WindowTime = e.EndTime;
                }
                return e.Payload;
            });
        }

        // get tweet text. RawTweet has it in different places.
        private static string GetText(RawTweet tweet)
        {
            if(tweet.extended_tweet != null && tweet.extended_tweet.full_text != null)
            {
                return tweet.extended_tweet.full_text;
            }

            if(tweet.retweeted_status != null && tweet.retweeted_status.extended_tweet != null && tweet.retweeted_status.extended_tweet.full_text != null)
            {
                return tweet.retweeted_status.extended_tweet.full_text;
            }
            
            return tweet.text;
        }
    }
}