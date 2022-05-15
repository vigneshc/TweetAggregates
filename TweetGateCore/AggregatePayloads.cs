using System;

namespace TweetGate.Core
{
    public class TweetDescription
    {
        public long Id { get; set; }
        public long FollowerCount { get; set; }
        public string Text { get; set; }
        public string ScreenName { get; set; }

        public long OriginalTweetId { get; set; }
    }

    public class Counts
    {
        public long WindowTime { get; set; }
        public ulong Count { get; set; }
    }

    public abstract class TopItems : IComparable
    {
        public long WindowTime { get; set; }
        public long FollowerCountSum { get; set; }
        public ulong TweetCount { get; set; }
        public int CompareTo(object obj) => this.FollowerCountSum.CompareTo(((TopItems)obj).FollowerCountSum);
    }

    public abstract class TopEntityItems : TopItems
    {
        public TweetDescription[] TopTweets { get; set; }
    }

    public class TopMentions : TopEntityItems
    {
        public string ScreenName { get; set; }
    }

    public class TopHashTags : TopEntityItems
    {
        public string HashTag { get; set; }
    }

    public class TopRetweets : TopItems
    {
        public long Id { get; set; }
        public string[] TopUsers { get; set; }
        public string Text { get; set; }
    }

    public class AggregateObservables
    {
        public IObservable<TopHashTags[]> TopHashtags { get; set; }
        public IObservable<TopMentions[]> TopMentions { get; set; }
        public IObservable<TopRetweets[]> TopRetweets { get; set; }
        public IObservable<Counts> HoppingCounts { get; set; }
        public IObservable<Counts> TumblingCounts { get; set; }
    }
}