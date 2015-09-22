package com.goober

import org.apache.spark.sql.catalyst.expressions.{Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object BatchBin {

  def locationToBucket(x: Double, y: Double): Int = {
    val numBucketsX = 2
    val numBucketsY = 2

    val col = Math.floor(x * numBucketsX).toInt
    val row = Math.floor(y * numBucketsY).toInt

    col * numBucketsX + row
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("goober")
    val sc = new SparkContext()
    val sqlc = new SQLContext(sc)

    val df = sqlc.read.json("hdfs://ec2-52-89-161-16.us-west-2.compute.amazonaws.com:9000/camus/topics/m2/hourly/2015/09/22/11/*")

    val normedStream = df.rdd.map(row => {
      val msgType = row.getAs[String]("_type")
      val rid = row.getAs[Long]("rideId")

      if (msgType == "TYPE1") {
        val locX = row.getAs[Row]("location").getAs[Double]("x")
        val locY = row.getAs[Row]("location").getAs[Double]("y")
        val loc = locationToBucket(locX, locY)

        (rid, (Some(loc), None))
      }

      else if (msgType == "TYPE2") {
        val eta = row.getAs[Double]("waitTime")

        (rid, (None, Some(eta)))
      }

      else {
        throw new RuntimeException()
      }
    })

    normedStream.take(10).foreach(println)
  }
}
