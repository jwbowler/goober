package com.goober

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.catalyst.expressions.{Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object BatchBin {

  type Loc = Int
  type Eta = Double

  def locationToBucket(lng: Double, lat: Double): Option[Loc] = {
    val numBucketsX = 2
    val numBucketsY = 2

    val minLng = -74.05
    val maxLng = -73.87
    val minLat = 40.68
    val maxLat = 40.89

    val scaledLng = (lng - minLng) / (maxLng - minLng)
    val scaledLat = (lat - minLat) / (maxLat - minLat)

    if (scaledLng < 0 || scaledLng >= 1 || scaledLat < 0 || scaledLat >= 1) {
      None
    }

    else {
      val col = Math.floor(scaledLng * numBucketsX).toInt
      val row = Math.floor(scaledLat * numBucketsY).toInt

      Some(col * numBucketsX + row)
    }
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

//    val log = Logger.getRootLogger()

    val conf = new SparkConf().setAppName("goober")
    val sc = new SparkContext()
    val sqlc = new SQLContext(sc)

//    val df = sqlc.read.json("hdfs://ec2-52-89-161-16.us-west-2.compute.amazonaws.com:9000/camus/topics/monday-test3/hourly/2015/09/24/16/*")

    val text = sc.textFile("hdfs://ec2-52-89-161-16.us-west-2.compute.amazonaws.com:9000/monday.csv")

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

    val normedStream = text.flatMap(line => {
      val words = line.split(",")

      val locX = words(2).toDouble
      val locY = words(3).toDouble
      val loc = locationToBucket(locX, locY)

      val timestampStr = words(1)
      val timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timestampStr)

      val waitTime = words(4).toDouble

      if (loc.isDefined) {
        Seq( (timestamp, loc.get, waitTime) )
      }

      else {
        Seq.empty[(Date, Loc, Eta)]
      }
    })

    normedStream.take(10).foreach(println)
  }
}
