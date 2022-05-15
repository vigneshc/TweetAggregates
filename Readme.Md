**Overview**<br/>
Continuously stream tweets containing a set of track terms. Aggregate top hashtags, top mentions from the stream and store them in a rocks db instance. Contains a local executable that can run forever, collecting aggregates and storing results in a local rocks DB. Also has a repl mode for querying the results from the db.


**Project Structure**
1. `TweetGateCore` contains classes for querying twitter, aggregating and storing results in DB.
2. `TweetGate` contains classes for the executable and commands described in *Usage* section.


**High level logic**

`TwitterStream.cs\StartTwitterPump()` Sends tweets to a pipe using [System.IO.Pipelines](https://docs.microsoft.com/en-us/dotnet/standard/io/pipelines). ==> `TwitterStream.cs\ProcessTweetStream()` Pushes tweets to a reactive subject.<br/>
TweetSubject ==> `Query.cs\SimpleAggregate()` returns a query that aggregates data and returns observables for aggregates ==> `RocksDBStore.cs\PersistObservableAsync()` stores aggregates to DB.<br/>
`Program.SaveAggregates.cs` kicks off above workflow.<br/>
[ReactiveX](https://reactivex.io/) is used for publish-subscribe mechanism and minimal [Trill](https://github.com/microsoft/Trill) is used for window aggregations.<br/>
[RocksDB Sharp](https://github.com/curiosity-ai/rocksdb-sharp) is used for storing aggregate data.

**Usage**

Either install dotnet and do “dotnet run” or build a self-sufficient executable.


1. Save tweets to a local file.<br/><br/>
*saveTweets [twitterConfigJsonFile] [destinationFile] [durationMinutes]*<br/>
Saves tweets to the file provided for duration minutes.<br/>

2. Compute Aggregates from a local file.<br/><br/>
*saveAggregates file [inputDataFile] [rocksDBPath]*<br/>
Aggregates tweets in `inputDataFile` and stores aggregates in DB.<br/>
Use case is for first storing the tweets in a file using (1) and then computing aggregates over it. Mainly used for testing.<br/><br/>

3. Compute aggregates for tweets directly from twitter stream api.<br/><br/>
*saveAggregates direct [twitterConfigJsonFile] [rocksDBPath]*<br/>
Streams tweets from twitter API, aggregates them and stores aggregates in DB.<br/>
Use case is for storing aggregates for certain keywords.<br/>

4. View aggregates in DB<br/><br/>
*repl [rocksDBPath] [OutputDirectoryPath]*<br/>
Apis for reading content in the DB. Additional details available in `Program.Repl.cs`<br/>
If `OutputDirectoryPath` is provided, results are stored in files in that directory. If it not provided, results are printed to console.
Use case is to quickly view DB content.

Example Twitter Config. [This page](https://developer.twitter.com/en/docs/twitter-api/v1/tweets/filter-realtime/guides/basic-stream-parameters) has more details on `TrackTerms`.<br/>

```json
{
    "TrackTerms": "comma,@separated,#hashTags,and,text",
    "OAuthConsumerSecret": "<>",
    "OAuthToken": "<>",
    "OAuthTokenSecret": "<>",
    "OAuthConsumerKey": "<>"
}

```
Example aggregates output for about 30 minutes is available here - [Top Hashtags](https://cloudflare-ipfs.com/ipfs/bafybeig4lstor5kuw3iqxda4wo2ggyxxrhwx5vkj3gijnen3422vz22nxy/gettophashtagsstring.637882371095256741.json) , [Top Mentions](https://cloudflare-ipfs.com/ipfs/bafybeicgczf7ludqcsncnwuad74msjjwkzqj3bzpxkihmpdr2f46mesncy/gettopmentionsstring.637882370892100277.json), [Top Retweets](https://cloudflare-ipfs.com/ipfs/bafybeiab7vfiy3qzldjvncdqq2hccugtkncukyxpfo6dgyu3335nsb2nte/gettopretweetsstring.637882372173132186.json).