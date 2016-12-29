package com.flipkart.uietnsbootcamp.gaurav

/**
  * Created by gaurav.malhotra on 27/12/16.
  */

import java.io._
import scala.io.Source
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import twitter4j.auth.Authorization
import twitter4j.Status
import twitter4j.auth.AuthorizationFactory
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.streaming.api.java.JavaStreamingContext

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.Function
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream

object TwitterAPIs {

  def main(args: Array[String]): Unit= {

    val lines = Source.fromFile("src/main/resources/twitter.txt").getLines.toList
    val keys = lines.map(line => {
                            line.split("=").tail.head.split("\"").tail.head })

    val map = lines.map(line => line.split("=").head -> line.split("=").tail.head.split("\"").tail.head).toMap

    val consumerKey = map("consumerKey")
    val consumerSecret = map("consumerSecret")
    val accessToken = map("accessToken")
    val accessTokenSecret = map("accessTokenSecret")
    val url = "https://stream.twitter.com/1.1/statuses/filter.json"

    val sparkConf = new SparkConf ().setAppName ("Twitter Streaming").setMaster("local")
    val sc = new SparkContext (sparkConf)



    // Twitter Streaming
    val ssc = new StreamingContext (sc, Seconds (2) )
    val checkpointDir = "/Users/gaurav.malhotra/Checkpoint"
    ssc.checkpoint(checkpointDir)

    val conf = new ConfigurationBuilder ()
    conf.setOAuthAccessToken (accessToken)
    conf.setOAuthAccessTokenSecret (accessTokenSecret)
    conf.setOAuthConsumerKey (consumerKey)
    conf.setOAuthConsumerSecret (consumerSecret)
    conf.setStreamBaseURL (url)
    conf.setSiteStreamBaseURL (url)

    val filter = Array ("Twitter", "Hadoop", "Big Data")

    val auth = AuthorizationFactory.getInstance (conf.build () )
    val tweets = TwitterUtils.createStream (ssc, Some(auth), filter)

    val statuses = tweets.map (status => status.getText)

    val hashtags = statuses.flatMap(status => status.split(" ").filter(_.startsWith("#")))

    val counts = hashtags.map(tag => (tag, 1)).reduceByKeyAndWindow(_+_ , _-_ , Seconds(60*5), Seconds(4))

    val sortedCounts = counts.map { case(tag, count) => (count, tag) }.transform(rdd => rdd.sortByKey(false))

    var i:Int = 1

    sortedCounts.foreachRDD(sortedCountRDD => {
      val file = new File("/Users/gaurav.malhotra/twitter_"+i);
      val writer = new FileWriter(file)

      writer.write("\nTop 10 hash tags:\n"+sortedCountRDD.top(10).mkString("\n"))
      writer.close()
      i +=1
    }
    )
    ssc.start ()
    ssc.awaitTermination()
  }
}
