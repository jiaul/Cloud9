import com.google.gson._

val eastTweets = sc.textFile("/shared/collections/twitter-2015-03-test-crawl/east/statuses.log.2015-03-07*.gz");

var eastTweetIds = eastTweets.filter(tweet => {
  val jsonParser = new JsonParser()
  !jsonParser.parse(tweet).getAsJsonObject().has("delete")
}).map(tweet => {
  val jsonParser = new JsonParser()
  jsonParser.parse(tweet).getAsJsonObject().get("id").getAsLong()
}).distinct()

val westTweets = sc.textFile("/shared/collections/twitter-2015-03-test-crawl/west/statuses.log.2015-03-07*.gz");

var westTweetIds = westTweets.filter(tweet => {
  val jsonParser = new JsonParser()
  jsonParser.setLenient(true);
  !jsonParser.parse(tweet).getAsJsonObject().has("delete")
}).map(tweet => {
  val jsonParser = new JsonParser()
  jsonParser.parse(tweet).getAsJsonObject().get("id").getAsLong()
}).distinct()

var intersection = eastTweetIds.intersection(westTweetIds)

eastTweetIds.count()
westTweetIds.count()
intersection.count()
System.exit(0)
