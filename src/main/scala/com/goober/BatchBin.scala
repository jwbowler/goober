package com.goober

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object BatchBin {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("goober")
    val sc = new SparkContext()
    val sqlc = new SQLContext(sc)

    val df = sqlc.read.json("hdfs://ec2-52-89-161-16.us-west-2.compute.amazonaws.com:9000/camus/topics/m2/hourly/2015/09/22/11/*")
    df.show()
  }
}
