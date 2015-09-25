package com.goober

import java.text.SimpleDateFormat
import java.util.{GregorianCalendar, Calendar, Date}

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.catalyst.expressions.{Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

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

//    val normedStream = df.rdd.map(row => {
//      val locX = row.getAs[String]("longitude").toDouble
//      val locY = row.getAs[String]("latitude").toDouble
//      val loc = locationToBucket(locX, locY)
//
//      val timestampStr = row.getAs[String]("timestamp")
//      val timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timestampStr)
//
//      val waitTime = row.getAs[String]("wait_time").toDouble
//
//      if (loc.isDefined) {
//        Seq( (timestamp, loc, waitTime) )
//      }
//
//      else {
//        Seq.empty[(Date, Loc, Eta)]
//      }
//    })

//    val normedStream = text.map(line => {
//      val words: Seq[String] = line.split(",")
//
//      val (lineNumber, rideId, timestamp, longitude, latitude, waitTime, msgType) = words
//
//      val locBucket = locationToBucket(longitude.toDouble, latitude.toDouble)
//
//      val timestampDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timestamp)
//      val timeBucket = timeToBucket(timestampDate)
//
//      val waitTime = words(4).toDouble
//
//      Seq( (loc.get, interval, waitTime) )
//    })

    val wordsStream = text.map(line => {
      line.split(",")
    })

//    val filteredByMsgTypeStream = wordsStream.filter(words => {
//      val (_, _, _, _, _, _, msgType: String) = words
//      msgType == "PKP"
//    })
//
//    val bucketedStream = filteredByMsgTypeStream.map(words => {
//      val (lineNumber, rideId, timestamp, longitude, latitude, waitTime, msgType) = words
//
//      val locBucket = locationToBucket(longitude.toDouble, latitude.toDouble)
//
//      val timestampDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timestamp)
//      val timeBucket = timeToBucket(timestampDate)
//    })
//
//    val blahStream = normedStream.map(record => {
//      val (loc, interval, waitTime) = record
//      val timeSeries = Vector.fill(86400){Vector()}
//
//      val timeSeries2 = timeSeries.updated(interval, Vector(waitTime))
//
//      timeSeries2
//    })

    wordsStream.take(2).foreach(println)
  }
}
