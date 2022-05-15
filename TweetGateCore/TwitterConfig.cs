using System.IO;
using Newtonsoft.Json;

namespace TweetGate.Core
{
    // Config needed to connect to twitter stream and get filtered status.
    public class TwitterConfig
    {
        public string OAuthToken { get; set; }
        public string OAuthTokenSecret { get; set; }
        public string OAuthConsumerKey { get; set; }
        public string OAuthConsumerSecret { get; set; }

        // https://developer.twitter.com/en/docs/twitter-api/v1/tweets/filter-realtime/guides/basic-stream-parameters
        public string TrackTerms { get; set; }

        public string Languages { get; set; }

        public string LogsDirectory { get; set; }

        public static TwitterConfig FromFile(string fileName)
        {
            return JsonConvert.DeserializeObject<TwitterConfig>(File.ReadAllText(fileName));
        }
    }
}