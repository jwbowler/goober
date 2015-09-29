package com.goober

import java.text.SimpleDateFormat

import akka.actor.ActorSystem
import com.goober.Util._
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.catalyst.expressions.{Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import scredis.{Redis, Client}

object BatchBin {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

//    val log = Logger.getRootLogger()

    val conf = new SparkConf().setAppName("goober")
    val sc = new SparkContext()
    val sqlc = new SQLContext(sc)

//    val df = sqlc.read.json("hdfs://ec2-52-89-161-16.us-west-2.compute.amazonaws.com:9000/camus/topics/monday-test3/hourly/2015/09/24/16/*")

    val text = sc.textFile("hdfs://ec2-52-25-69-74.us-west-2.compute.amazonaws.com:9000/monday-all.csv")

    val words = text.map(line => {
      val words = line.split(",")
      (words(1), words(6), words)
    }).filter(tuple => {
      val (rideId, msgType, _) = tuple
      rideId != "ride_id" && msgType == "REQ"
    }).map(pair => {
      val (_, _, words) = pair

      val rideId = words(1).toLong
      val timestamp = words(2)
      val longitude = words(3).toDouble
      val latitude = words(4).toDouble
      val waitTime = words(5).toLong
      val msgType = words(6)

      val locBucket = locationToBucket(longitude.toDouble, latitude.toDouble)

      val timestampDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timestamp)
      val timeBucket = timeToBucket(timestampDate)

      (timeBucket, locBucket, waitTime)
    }).filter(tuple => {
      val (timeBucket, locBucket, waitTime) = tuple
      timeBucket.isDefined && locBucket.isDefined
    }).map(tuple => {
      val (timeBucket, locBucket, waitTime) = tuple
      (timeBucket.get, locBucket.get, waitTime)
    })

    val splitEtas = words.flatMap(record => {
      val (timeBucket, locBucket, waitTime) = record
      for (eta: Long <- List.range(waitTime, 0, -1)) yield {
        val k = (timeBucket + (eta - waitTime), locBucket)
        val v = (eta.toDouble, 1)
        (k, v)
      }
    })

    val aggregates = splitEtas.reduceByKey((tuple1, tuple2) => {
      val (eta1, count1) = tuple1
      val (eta2, count2) = tuple2

      val avg = ((eta1 * count1) + (eta2 * count2)) / (count1 + count2)
      val count = count1 + count2

      (avg, count)
    })

    val groupedByLocation = aggregates.map(pair => {
      val (k, v) = pair
      val (avg, count) = v
      val (timeBucket, locBucket) = k

      (timeBucket, Map(locBucket.toString -> avg))
    }).reduceByKey(_ ++ _)

//    groupedByLocation.take(1).foreach(println)

//    println(groupedByLocation.countByKey())

    groupedByLocation.foreachPartition(part => {

      val redisClient = new RedisClient("john-redis.2wlafm.ng.0001.usw2.cache.amazonaws.com", 6379)
      part.foreach(pair => {
        val (k, v) = pair
        redisClient.hmset(k, v)
      })
//      implicit val system = ActorSystem("my-actor-system")
//
//      val redisConfig = ConfigFactory.load("scredis.conf").getConfig("scredis")
//      val redisClient = Client(redisConfig)
//
//      import redisClient.dispatcher
//
//      val log = Logger.getRootLogger()
//      log.info("WOO")
//
//      part.foreach(record => {
//        val (k, v) = record
//
//        redisClient.hmSet(k.toString, v)
//      })
    })
  }
}
