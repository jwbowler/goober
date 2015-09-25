import java.text.SimpleDateFormat

import com.goober.Util._
import com.redis.RedisClient
import com.tdunning.math.stats.{AVLTreeDigest, ArrayDigest}
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import play.api.libs.json.{JsValue, Json}

object StreamBin {

  def rideTableUpdateFunction(in: Seq[LocEtaPair], lastState: Option[LocEtaPair]): Option[LocEtaPair] = {

    in.foldLeft(lastState) {
      (state, newPair) => {
        val (newLoc, newEta, timestamp): LocEtaPair = newPair

        // ride is already in table
        if (state.isDefined) {
          val (oldLoc, oldEta, timestamp) = state.get

          // handle pickup/cancel
          if (newLoc.isEmpty && newEta.isEmpty) {
            None
          }

          // handle location or ETA update
          else {
            val loc = newLoc.orElse(oldLoc)
            val eta = newEta.orElse(oldEta)
            Some((loc, eta, timestamp))
          }
        }

        // ride is not already in table
        else {

          // if "new ride", send all data
          if (newLoc.isDefined || newEta.isDefined) {
            Some(newPair)
          }

          // otherwise, something's weird, send no data
          else {
            None
          }
        }

      }
    }
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val brokers = "ec2-52-88-49-174.us-west-2.compute.amazonaws.com:9092"
    val topics = "m2"
    val topicsSet = topics.split(",").toSet

    val conf = new SparkConf().setAppName("goober")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    val sqlc = new SQLContext(sc)

    ssc.checkpoint("/tmp")

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val msgStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val wordsStream = msgStream.map(pair => {
      val (_, str) = pair
      str.split(",")
    })

    val normedStream = wordsStream.map[UserRecord](words => {

//      val lineNumber = words(0)
      val rideId = words(1).toLong
      val timestamp = words(2)
      val longitude = words(3).toDouble
      val latitude = words(4).toDouble
      val waitTime = words(5).toInt
      val msgType = words(6)

      val locBucket = locationToBucket(longitude.toDouble, latitude.toDouble)

      val timestampDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timestamp)
      val timeBucket = timeToBucket(timestampDate)

      val ts = timestampDate.getTime.toString

      if (msgType.contains("REQ")) {
        (rideId, (locBucket, None, ts))
      }

      else if (msgType.contains("ETA")) {
        (rideId, (None, Some(waitTime.toDouble), ts))
      }

//      else if (msgType.contains("PKP")) {
//        (Some(rideId), (None, None, ts))
//      }

      else {
        (rideId, (None, None, ts))
//        (None, (None, None, ts))
      }
    })
//      .filter(blah => {
//      val (a: Option[Uid], (b, c, d)) = blah
//      a.isDefined
//    })

    val rideTableStream = normedStream.updateStateByKey(rideTableUpdateFunction)

    val cleanRideTableStream = rideTableStream.filter(record => {
      val (uid, (loc, eta, timestamp)) = record
      loc.isDefined && eta.isDefined
    })

    val locTableStream = cleanRideTableStream.map[(Loc, (Eta, String))](record => {
      val (uid, (loc, eta, timestamp)) = record
      (loc.get, (eta.get, timestamp))
    }).groupByKey()

//    val avgStream = locTableStream.map[(Loc, Double)](pair => {
//      val (loc, etaList) = pair
//      val avg = etaList.sum / etaList.size
//      (loc, avg)
//    })

    val percentileStream = locTableStream.map[(Loc, (Double, Double, String))](pair => {
      val (loc, stuffList) = pair

      val (etaList, timestampList) = stuffList.unzip

      val avg = etaList.sum / etaList.size

      val tDigest = new AVLTreeDigest(100)
      etaList.foreach(tDigest.add(_, 1))
      val q90 = tDigest.quantile(0.9)

      (loc, (avg, q90, timestampList.head))
    })

    val outputStream = percentileStream.map(pair => {
      val (k, (avg, p90, timestamp)) = pair
      ("stream", "" + k + "/" + avg.toInt + "/" + p90.toInt + "/" + timestamp)
    })

//    outputStream.print()

    outputStream.foreachRDD(rdd => {
      rdd.foreachPartition(pairs => {
        val redisClient = new RedisClient("john-redis.2wlafm.ng.0001.usw2.cache.amazonaws.com", 6379)
        pairs.foreach(pair => {
          val (k, v) = pair
          redisClient.publish(k, v.toString)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}