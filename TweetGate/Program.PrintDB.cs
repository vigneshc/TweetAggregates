using System.Threading.Tasks;
using System.Reactive.Linq;

using TweetGate.Core;
using Newtonsoft.Json;

namespace TweetGate
{
    partial class Program
    {
        // Prints DB summary and recent data.
        static Task PrintDB(string[] args)
        {
            if(args.Length != 3)
            {
                logWriter.WriteLine("Usage: TweetGate printDB outputDBPath count");
                return Task.CompletedTask;
            }

            int count = int.Parse(args[2]);
            var store = new RocksDBStore(args[1], isReadOnly: true, logAction: logWriter.WriteLine);
            store.Initialize();
            store.GetRecentCounts(count)
            .ToObservable()
            .ForEachAsync(e => logWriter.WriteLine(JsonConvert.SerializeObject(e, Formatting.Indented)))
            .Wait();

            store.GetRecentTopMentionsString(count)
            .ToObservable()
            .ForEachAsync(e => logWriter.WriteLine(e))
            .Wait();

            store.GetRecentTopHashTagsString(count)
            .ToObservable()
            .ForEachAsync(e => logWriter.WriteLine(e))
            .Wait();

            store.GetRecentTopRetweetsString(count)
            .ToObservable()
            .ForEachAsync(e => logWriter.WriteLine(e))
            .Wait();

            logWriter.WriteLine(JsonConvert.SerializeObject(store.GetSummary(), Formatting.Indented));

            return Task.CompletedTask;
        }
    }
}
