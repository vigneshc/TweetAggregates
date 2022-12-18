
using System.IO.Pipelines;
using System.Reactive.Linq;
using TweetGate.Core;
namespace TweetAggregateTests;


[TestClass]
public class TwitterStreamTests
{
    // sanity test to validate CI.
    [TestMethod]
    public void GetTweetsEnumerable_Test()
    {
        var pipe = new Pipe();
        using(Stream writeStream = new MemoryStream())
        using(var tweetWriter = new StreamWriter(writeStream))
        {
            tweetWriter.WriteLine("1");
            tweetWriter.WriteLine("2");
            tweetWriter.Flush();
            writeStream.Seek(0, SeekOrigin.Begin);
            var result = TwitterStream.GetTweetsEnumerable(writeStream).ToArray();
            Assert.AreEqual(2, result.Length);
            Assert.AreEqual("1", result[0]);
        }
    }
}