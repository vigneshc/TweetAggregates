using System;

namespace TweetGate.Core
{
    // stores aggregate results in a rocks DB instance.
    public sealed partial class RocksDBStore : IDisposable
    {
        // Summary of DB content.
        public class DbSummary
        {
            public DateTime MinDate { get; set; }
            public DateTime MaxDate { get; set; }
            public TimeSpan Duration => this.MaxDate - this.MinDate;

            public ulong WindowCount { get; set; }

            public ulong NumberOfTweets { get; set; }

            public DbSummary Accumulate(Counts count)
            {
                if(this.MinDate == DateTime.MinValue)
                {
                    this.MinDate = new DateTime(count.WindowTime, DateTimeKind.Utc);
                }

                this.MaxDate = new DateTime(count.WindowTime, DateTimeKind.Utc);
                this.WindowCount++;
                this.NumberOfTweets += count.Count;
                return this;
            }
        }
    }
}