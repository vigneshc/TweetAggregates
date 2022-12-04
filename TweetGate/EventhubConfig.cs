using System.IO;
using Newtonsoft.Json;

namespace TweetGate
{
    internal class EventhubConfig
    {
        public string ConnectionString { get; set; }
        public string EventhubName { get; set; }

        public bool GzipCompressed { get; set; }

        public long MaxEventhubMessageSizeInBytes { get; set; }

        public uint MaxTweetAccumulationTimeInSeconds { get; set; }

        public uint ProducerMaximumWaitTimeInSeconds { get; set; }

        public static EventhubConfig Parse(string fileName) => JsonConvert.DeserializeObject<EventhubConfig>(File.ReadAllText(fileName));
    }
}