import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Duration, Seconds}
import org.apache.spark.streaming.kafka.KafkaUtils

object StreamBin {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val brokers = "ec2-52-88-49-174.us-west-2.compute.amazonaws.com:9092"
    val topics = "my-topic"
    val topicsSet = topics.split(",").toSet

    val conf = new SparkConf().setAppName("goober")
    val ssc = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    messages.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
}