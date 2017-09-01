import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import com.google.gson.{Gson, GsonBuilder, JsonParser}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object Utils {

  val numFeatures = 1000

  val tf = new HashingTF(numFeatures)

  def featurize(s: String) = {

    tf.transform(s.sliding(2).toSeq)

  }

}

object TestScala {

  def setTwitterKeys() = {
    // Configure Twitter credentials

    val apiKey = "czxjBla41EHoljofyWTZ35VJz"

    val apiSecret = "g47ZkEM1COmMr5QNWkbuaynwc0Z5FRD3YeUSNifaSoZhQ4fWBu"

    val accessToken = "378038601-7C9wYbOqQgHddahOm0XK69sxwAIgi8YH4peWcguX"

    val accessTokenSecret = "bMgXFnhxFD52JXSAExINXTGhs5NxZge355kJBviDVr0fb"

    System.setProperty("twitter4j.oauth.consumerKey", apiKey)

    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)

    System.setProperty("twitter4j.oauth.accessToken", accessToken)

    System.setProperty("twitter4j.oauth.accessTokenSecret",

      accessTokenSecret)
  }

  def writeTweets(stream: ReceiverInputDStream[twitter4j.Status]) = {
    val jsoned_tweet = stream.map(new Gson().toJson(_))
    val outputDirectory = "/Users/ildarsafin/Documents/tweets"

    val numTweetsCollect = 1000L
    var numTweetsCollected = 0L
    var partNumber = 1

    jsoned_tweet.foreachRDD((rdd, time) => {

      val count = rdd.count()

      if (count > 0) {

        val outputRDD = rdd.repartition(1)

        outputRDD.saveAsTextFile(outputDirectory + "/tweets_" +

          time.milliseconds.toString)

        numTweetsCollected += count

        if (numTweetsCollected > numTweetsCollect) {

          System.exit(0)

        }
      }
    })
  }

  def makeModelFromTweets(sc: SparkContext) = {
    val sqlContext = new SQLContext(sc)

    val gson = new GsonBuilder().setPrettyPrinting().create()

    val jsonParser = new JsonParser()

    val tweetInput = "/Users/ildarsafin/Documents/tweets/tweets_[0-9]*/part-[0-9]*"

    val tweetTable = sqlContext.jsonFile(tweetInput)

    tweetTable.registerTempTable("tweetTable")

    tweetTable.printSchema()

    val texts = sqlContext

      .sql("SELECT text from tweetTable")

      .rdd

      .map(row => row.get(0).toString)

    // Cache the vectors RDD since it will be used for all the KMeans iterations.

    val vectors = texts

      .map(Utils.featurize)

      .cache()

    val numClusters = 10

    val numIterations = 40

    vectors.count() // Calls an action on the RDD to populate the vectors cache.

    // Train KMenas model and save it to file

    val model: KMeansModel = KMeans.train(vectors, numClusters, numIterations)

    model.save(sc, "/Users/ildarsafin/Documents/tweets_model/")


    println("----100 example tweets from each cluster")

    0 until numClusters foreach { i =>

      println(s"\nCLUSTER $i:")

      texts.take(100) foreach { t =>

        if (model.predict(Utils.featurize(t)) == i) println(t)

      }

    }
  }

  def countHashtags(stream: ReceiverInputDStream[twitter4j.Status]) = {
    val tweets = stream.map(t => t.getText)
    var words = tweets.map(x => x.split(" "))

    val hashtags = words.flatMap(t => t.filter(t => t.startsWith("#")))

    val counts = hashtags.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }

    tweets.print()
    hashtags.print()

    counts.print()
  }

  def analyseTweets(sc: SparkContext, ssc: StreamingContext) = {
    println("Initializing Twitter stream...")

    val tweets = TwitterUtils.createStream(ssc, None)

    val statuses = tweets.map(_.getText)

    val modelFile = "/Users/ildarsafin/Documents/tweets_model/"

    println("Initializing the KMeans model...")

    val model = KMeansModel.load(sc, modelFile)

    val clusterNumber = 0 //arabic language

    val filteredTweets = statuses

      .filter(t => model.predict(Utils.featurize(t)) == clusterNumber)

    filteredTweets.print()
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    conf.setAppName("spark-sreaming")

    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(1))

    setTwitterKeys

    // Create Twitter Stream
    val stream = TwitterUtils.createStream(ssc, None)


    //    writeTweets(stream)
    //    makeModelFromTweets(sc)
    analyseTweets(sc, ssc)

    ssc.start()

    ssc.awaitTermination()

  }

}
