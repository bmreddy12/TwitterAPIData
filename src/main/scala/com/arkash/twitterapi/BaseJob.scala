package com.arkash.twitterapi

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.{Status, TwitterFactory}
import twitter4j.ResponseList
import twitter4j.conf.ConfigurationBuilder

object BaseJob {
  def main(args: Array[String]): Unit = {

    val consumerKey = "EwGQXqJ1zf7BuZ3yF6wrxpj20" // Your consumerKey
    val consumerSecret = "jFyuAkJ5vcjBbKU7Uwcdo0kDokFSIfQqLgRJZ8Lk8IBTY9SNYD" // your API secret
    val accessToken = "1306832472026624000-oJiaSiPL7eZ8GTAPpGchiog02WWn3G" // your access token
    val accessTokenSecret = "DoxjBC8MCsIJOOi39TNz9FZUrDqPXK0fbwziMRaw7SUY5" // your token secret

    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)

    val tf = new TwitterFactory(cb.build())


    val twitter: twitter4j.Twitter = tf.getInstance()
    val statuses = twitter.getHomeTimeline

    val it = statuses.iterator()
    while(it.hasNext){
      val status = it.next()
      println("User: "+status.getUser.getName+", ScreenName:"+status.getUser.getScreenName+", ID: "+status.getUser.getId+", Text: "+status.getText)
    }




  }


}
