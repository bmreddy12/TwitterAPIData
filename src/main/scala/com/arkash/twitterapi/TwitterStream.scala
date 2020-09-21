package com.arkash.twitterapi

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import twitter4j.Status
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object TwitterStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Twitter Data")
    val sc =new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(10000))

    val consumerKey = "EwGQXqJ1zf7BuZ3yF6wrxpj20" // Your consumerKey
    val consumerSecret = "jFyuAkJ5vcjBbKU7Uwcdo0kDokFSIfQqLgRJZ8Lk8IBTY9SNYD" // your API secret
    val accessToken = "1306832472026624000-oJiaSiPL7eZ8GTAPpGchiog02WWn3G" // your access token
    val accessTokenSecret = "DoxjBC8MCsIJOOi39TNz9FZUrDqPXK0fbwziMRaw7SUY5" // your token secret

    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)

    val auth = new OAuthAuthorization(cb.build)
    val tweets = TwitterUtils.createStream(ssc, Some(auth))
    val englishTweets = tweets.filter(_.getUser.getId == 14861004)
    val statuses = englishTweets.map(status => (status.getText(),status.getUser.getName(),status.getUser.getScreenName(),status.getCreatedAt.toString))


    statuses.foreachRDD { (rdd, time) =>

      rdd.foreachPartition { partitionIter =>
        val props = new Properties()
        val bootstrap = "localhost:9092" //-- your external ip of GCP VM, example: 10.0.0.1:9092
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("bootstrap.servers", bootstrap)
        val producer = new KafkaProducer[String, String](props)
        partitionIter.foreach { elem =>
          val dat = elem.toString()
          val data = new ProducerRecord[String, String]("tweetLang", null, dat) // "llamada" is the name of Kafka topic
          producer.send(data)
        }
        //producer.flush()
        producer.close()
      }
    }
    ssc.start()
    ssc.awaitTermination()


  }

}
