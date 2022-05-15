using System;
using System.Collections.Generic;
using System.Reactive.Linq;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.StreamProcessing;
using Newtonsoft.Json;
using RocksDbSharp;

namespace TweetGate.Core
{
    // stores aggregate results in a rocks DB instance.
    // PersistObservableAsync() method describes key structure.
    public sealed partial class RocksDBStore : IDisposable
    {
        const string CountsDBPath = "counts";
        const string HashTagsDBPath = "hashtags";
        const string MentionsDBPath = "mentions";
        const string RetweetsDBPath = "retweets";

        private readonly string storePath;
        private readonly bool isReadOnly;
        private readonly ColumnFamilies columnFamilies;
        private Action<string> logAction;
        private ColumnFamilyHandle countsFamily;
        private ColumnFamilyHandle retweetsFamily;
        private ColumnFamilyHandle mentionsFamily;
        private ColumnFamilyHandle hashTagsFamily;
        private RocksDb rocksDb;

        public RocksDBStore(string storeRootPath, bool isReadOnly, Action<string> logAction)
        {
            this.storePath = storeRootPath;
            this.logAction = logAction;
            this.isReadOnly = isReadOnly;
            this.columnFamilies = new ColumnFamilies()
                {
                    { CountsDBPath, new ColumnFamilyOptions().SetComparator(new StringComparator())},
                    { RetweetsDBPath, new ColumnFamilyOptions().SetComparator(new StringComparator())},
                    { MentionsDBPath, new ColumnFamilyOptions().SetComparator(new StringComparator())},
                    { HashTagsDBPath, new ColumnFamilyOptions().SetComparator(new StringComparator())},
                };
            this.logAction($"Using db path: {storeRootPath}");
        }

        public void Initialize()
        {
            var options = new DbOptions()
            .SetCreateIfMissing(!this.isReadOnly)
            .SetCreateMissingColumnFamilies(true);
            
            if(this.isReadOnly)
            {
                this.rocksDb =  RocksDb.OpenReadOnly(options, this.storePath, this.columnFamilies, errIfLogFileExists: false);
            }
            else
            {
                this.rocksDb =  RocksDb.Open(options, this.storePath, this.columnFamilies);
            }

            this.countsFamily = this.rocksDb.GetColumnFamily(CountsDBPath);
            this.retweetsFamily = this.rocksDb.GetColumnFamily(RetweetsDBPath);
            this.mentionsFamily = this.rocksDb.GetColumnFamily(MentionsDBPath);
            this.hashTagsFamily = this.rocksDb.GetColumnFamily(HashTagsDBPath);
        }
        
        // creates task that continously persists results from multiple aggregate observables.
        public async Task PersistObservableAsync(AggregateObservables aggregateObservables, CancellationToken token)
        {
            // Store top retweets. Key == Timestamp , Value == TopReTweets[] in json.
            Task retweetsTask = aggregateObservables.TopRetweets
            .ForEachAsync(
                e => this.rocksDb.Put(
                    Encoding.UTF8.GetBytes(e[0].WindowTime.ToString("D19")),
                    Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(e, Formatting.Indented)), // e is TopReTweets[]
                    cf : this.retweetsFamily),
                token);

            // Store counts. Key == Timestamp, Value = Count.
            Task countsTask = aggregateObservables.HoppingCounts
            .ForEachAsync(
                e => this.rocksDb.Put(
                    Encoding.UTF8.GetBytes(e.WindowTime.ToString("D19")), 
                    BitConverter.GetBytes(e.Count), cf: this.countsFamily),
                token);
            
            // emit hearbeat log for this task for debugging.
            Task logTask = aggregateObservables.TumblingCounts
            .Scan((0UL, 0L), (count, newCounts) => (count.Item1 + newCounts.Count, newCounts.WindowTime))
            .ForEachAsync(u => this.logAction($"RocksDBStore.PersistObservableAsync LocalTime: {DateTime.UtcNow:o} - WindowTime: {new DateTime(u.Item2, DateTimeKind.Utc):o} TotalTweetsProcessed: {u.Item1}"), token);
            
            // store top mentions. Key == windowTime+screenName. Value == TopMentions
            // enables querying top mentions for a particular screen name for a time range.
            Task topmentionsTask = aggregateObservables.TopMentions
            .ForEachAsync(e => 
            {
                WriteBatch batch = new WriteBatch();
                foreach(TopMentions t in e)
                {
                    batch.Put(
                        GetKey(t.WindowTime, t.ScreenName), 
                        Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(t, Formatting.Indented)), 
                        cf: this.mentionsFamily);
                }

                this.rocksDb.Write(batch);
            },
            token);

            // store top hashtags. Key == windowTime+hashTag , Value = TopHashTags
            // enables querying top hashtags for a particular hash tag for a time range.
            Task topHashTagTask = aggregateObservables.TopHashtags
            .ForEachAsync(e => 
            {
                WriteBatch batch = new WriteBatch();
                foreach(TopHashTags t in e)
                {
                    batch.Put(
                        GetKey(t.WindowTime, t.HashTag), 
                        Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(t, Formatting.Indented)), cf : this.hashTagsFamily);
                }

                this.rocksDb.Write(batch);
            },
            token);

            // wait for the task
            await Task.WhenAny(retweetsTask, countsTask, topmentionsTask, topHashTagTask, logTask);
            this.logAction($"One of the aggregates task exited. RetweetsTask :{retweetsTask.IsCompleted} CountsTask : {countsTask.IsCompleted} TopMentionsTask : {topmentionsTask.IsCompleted} TopHashTagTask : {topHashTagTask.IsCompleted} LogTask : {logTask.IsCompleted}");

            await Task.WhenAll(retweetsTask, countsTask, topmentionsTask, topHashTagTask, logTask);
            return;
        }

        // returns summary of db content.
        public DbSummary GetSummary()
        {
            return GetCounts(DateTime.MinValue, DateTime.MaxValue)
            .Aggregate(new DbSummary(), (oldSummary, count) => oldSummary.Accumulate(count));
        }

        // tweet counts for a given time range.
        public IEnumerable<Counts> GetCounts(DateTime startTime, DateTime endTime)
        {
            return GetData(startTime, endTime, this.countsFamily, subKey: null)
            .Select(t => new Counts(){ WindowTime = t.Item1, Count = BitConverter.ToUInt64(t.Item2, 0)});
        }

        // top mentions for given time range.
        // if screenName is provided, result will be restricted to the screenName.
        public IEnumerable<string> GetTopMentionsString(DateTime startTime, DateTime endTime, string screenName)
        {
            return GetData(startTime, endTime, this.mentionsFamily, subKey: screenName)
            .Select(t => Encoding.UTF8.GetString(t.Item2));
        }

        // top hash tags for a given time range.
        // if hashTag is provided, result will be restricted to the hashTag.
        public IEnumerable<string> GetTopHashTagsString(DateTime startTime, DateTime endTime, string hashTag)
        {
            return GetData(startTime, endTime, this.hashTagsFamily, subKey: hashTag)
            .Select(t => Encoding.UTF8.GetString(t.Item2));
        }

        // top retweets for a time range.
        public IEnumerable<string> GetTopRetweetsString(DateTime startTime, DateTime endTime)
        {
            return GetData(startTime, endTime, this.retweetsFamily, subKey: null)
            .Select(t => Encoding.UTF8.GetString(t.Item2));
        }

        public IEnumerable<Counts> GetRecentCounts(int count)
        {
            return GetRecentData(count, this.countsFamily)
            .Select(t => new Counts(){ WindowTime = t.Item1, Count = BitConverter.ToUInt64(t.Item2, 0)});
        }

        public IEnumerable<string> GetRecentTopMentionsString(int count)
        {
            return GetRecentData(count, this.mentionsFamily)
            .Select(t => Encoding.UTF8.GetString(t.Item2));
        }

        public IEnumerable<string> GetRecentTopHashTagsString(int count)
        {
            return GetRecentData(count, this.hashTagsFamily)
            .Select(t => Encoding.UTF8.GetString(t.Item2));
        }

        public IEnumerable<string> GetRecentTopRetweetsString(int count)
        {
            return  GetRecentData(count, this.retweetsFamily)
            .Select(t => Encoding.UTF8.GetString(t.Item2));
        }

        public void Dispose()
        {
            if(this.rocksDb != null)
            {
                this.rocksDb.Dispose();
                this.rocksDb = null;
            }
        }

        private byte[] GetKey(long ticks, string item)
        {
            return Encoding.UTF8.GetBytes($"{ticks:D19}{item}");
        }

        // stream recent data.
        private IEnumerable<(long, byte[])> GetRecentData(int count, ColumnFamilyHandle columnFamily)
        {
            using(var iterator = this.rocksDb.NewIterator(cf: columnFamily))
            {
                
                long ticks = 0;
                int recordCount = 0;
                for
                (
                    iterator.SeekToLast();
                    iterator.Valid() && recordCount < count;
                    iterator.Prev(), recordCount++
                )
                {
                    var key = Encoding.UTF8.GetString(iterator.Key());
                    ticks = long.Parse(key.Substring(0, 19));
                    yield return (ticks, iterator.Value());
                }
            }
        }

        // get data for a time range for a column family.
        // Keys are timestamps. Returns data in time order.
        private IEnumerable<(long, byte[])> GetData(DateTime startTime, DateTime endTime, ColumnFamilyHandle columnFamily, string subKey)
        {
            using(var iterator = this.rocksDb.NewIterator(cf: columnFamily))
            {
                long ticks = 0;
                var startingKey = startTime.Ticks.ToString("D19");
                if(subKey != null)
                {
                    startingKey = $"{startingKey}{subKey}";
                }
                
                for
                (
                    iterator.Seek(Encoding.UTF8.GetBytes(startingKey));
                    iterator.Valid() && ticks < endTime.Ticks;
                    iterator.Next()
                )
                {
                    var key = Encoding.UTF8.GetString(iterator.Key());
                    ticks = long.Parse(key.Substring(0, 19));
                    if(ticks >= endTime.Ticks)
                    {
                        yield break;
                    }

                    yield return (ticks, iterator.Value());
                }
            }
        }
    }
}