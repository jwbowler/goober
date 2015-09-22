import com.redis.RedisClient
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamBin {

  type Uid = Long
  type Loc = Int
  type Eta = Double

  type LocEtaPair = (Option[Loc], Option[Eta])
  type UserRecord = (Uid, LocEtaPair)
  type UidEtaPair = (Uid, Eta)
  type EtaList = Seq[Eta]

  def locationToBucket(x: Double, y: Double): Int = {
    val numBucketsX = 2
    val numBucketsY = 2

    val col = Math.floor(x * numBucketsX).toInt
    val row = Math.floor(y * numBucketsY).toInt

    col * numBucketsX + row
  }

  def rideTableUpdateFunction(in: Seq[LocEtaPair], lastState: Option[LocEtaPair]): Option[LocEtaPair] = {

    in.foldLeft(lastState) {
      (state, newPair) => {
        val (newLoc, newEta): LocEtaPair = newPair

        // ride is already in table
        if (state.isDefined) {
          val (oldLoc, oldEta) = state.get

          // handle pickup/cancel
          if (newLoc.isEmpty && newEta.isEmpty) {
            None
          }

          // handle location or ETA update
          else {
            val loc = newLoc.orElse(oldLoc)
            val eta = newEta.orElse(oldEta)
            Some((loc, eta))
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
    val topics = "m1"
    val topicsSet = topics.split(",").toSet

    val conf = new SparkConf().setAppName("goober")
    val ssc = new StreamingContext(conf, Seconds(1))

    ssc.checkpoint("/tmp")

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val msgStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val normedStream = msgStream.map[UserRecord](record => {
      val (_, str) = record
      val words = str.split(" ")

      if (words(0) == "TYPE1") {

        val rid = words(1).toLong
        val loc = Some(locationToBucket(words(3).toDouble, words(4).toDouble))
        val eta = None

        (rid, (loc, eta))

      } else {

        val rid = words(1).toLong
        val loc = None
        val eta = Some(words(2).toDouble)

        (rid, (loc, eta))
      }

    })

    val rideTableStream = normedStream.updateStateByKey(rideTableUpdateFunction)

    val cleanRideTableStream = rideTableStream.filter(record => {
      val (uid, (loc, eta)) = record
      loc.isDefined && eta.isDefined
    })

    val locTableStream = cleanRideTableStream.map[(Loc, Eta)](record => {
      val (uid, (loc, eta)) = record
      (loc.get, eta.get)
    }).groupByKey()

    val avgStream = locTableStream.map[(Loc, Double)](pair => {
      val (loc, etaList) = pair
      val avg = etaList.sum / etaList.size
      (loc, avg)
    })

    val outputStream = avgStream.map(pair => {
      val (k, v) = pair
      ("loc_" + k + "_avg", v)
    })

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