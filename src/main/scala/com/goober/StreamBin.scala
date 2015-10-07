import com.goober.Util._
import com.redis.RedisClient
import com.tdunning.math.stats.AVLTreeDigest
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamBin {

  def rideTableUpdateFunction(in: Seq[MessageTuple1], lastState: Option[LocEtaPair]): Option[LocEtaPair] = {

    in.foldLeft(lastState) {
      (state, newTuple) => {
        val (newType, newLoc, newEta) = newTuple

        newType match {

          // if a ride is being picked up, don't keep state for it
          case MessageType.PICKUP => None

          // otherwise, if this is either a ride request or an ETA message, update the state of this ride with whatever
          // new data we have
          case _ => {
            if (state.isDefined) {
              val (oldLoc, oldEta) = state.get

              val loc = newLoc.orElse(oldLoc)
              val eta = newEta.orElse(oldEta)

              Some((loc, eta))
            } else {
              Some((newLoc, newEta))
            }
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

    val sc = new SparkContext()
    val ssc = new StreamingContext(sc, Seconds(1))

    ssc.checkpoint("hdfs://ec2-52-25-69-74.us-west-2.compute.amazonaws.com:9000/tmp")

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    // Tokenize messages. Persist the stream, so we can use it for stream processing and also separately as a clock
    // signal.
    val wordsStream = kafkaStream.map(pair => {
      val (_, str) = pair
      str.stripLineEnd.split(",")
    }).persist()


    /*
     * DSTREAM BRANCH 1: Clock signal
     */

    // Find the most recent timestamp in each RDD
    val lastTimestampStream = wordsStream.map(words => {
      val timestamp = words(2)
      timestampToBucket(timestamp)
    }).reduce(math.max)

    // Broadcast this most-recent-timestamp through Redis
    lastTimestampStream.foreachRDD(rdd => {
      val redisClient = new RedisClient(redisHost, redisPort);
      redisClient.publish("time", rdd.first().toString)
    })


    /*
     * DSTREAM BRANCH 2: Main computation
     */

    // Key messages by rideId. Keep message-type, location-bucket, and ETA as relevant data.
    //
    // ("Pickup" messages have a "wait time" field instead of an "eta" field, which we don't need.)
    val normedStream = wordsStream.map(words => {
      val rideId = words(1).toLong
      val timestamp = words(2)
      val longitude = words(3).toDouble
      val latitude = words(4).toDouble
      val eta = words(5).toDouble
      val msgType = MessageType.fromMessageString(words(6))

      val locBucket = locationToBucket(longitude.toDouble, latitude.toDouble)

      msgType match {
        case MessageType.PICKUP => (rideId, (msgType, locBucket, None))
        case _                  => (rideId, (msgType, locBucket, Some(eta)))
      }
    })

    // Use the incoming message stream to maintain state (location and ETA) for every requested ride.
    //
    // For each microbatch, this DStream will return an RDD of key-value pairs, where each key is a RideId that is
    // currently active (i.e. there is a car driving to pick up a passenger right now), and the value is a tuple of
    // (location-bucket, last-updated-ETA).
    val rideStateStream = normedStream.updateStateByKey(rideTableUpdateFunction)

    // Filter out rides with unknown location or ETA; then, group ETA's by location bucket.
    val etaLists = rideStateStream.filter(pair => {
      val (uid, (loc, eta)) = pair
      loc.isDefined && eta.isDefined
    }).map(pair => {
      val (uid, (loc, eta)) = pair
      (loc.get, eta.get)
    }).groupByKey()

    // Calculate summary statistics for each key.
    val stats = etaLists.map(pair => {
      val (k, etaList) = pair

      val count = etaList.size
      val avg = etaList.sum / etaList.size

      val tDigest = new AVLTreeDigest(100)
      etaList.foreach(tDigest.add(_, 1))
      val p90 = tDigest.quantile(0.9)

      (k, (count, avg, p90))
    })

    // Load a hash map from location buckets to summary statistics into the Redis key "current".
    stats.foreachRDD(rdd => {
      rdd.foreachPartition(pairs => {
        val redisClient = new RedisClient(redisHost, redisPort)
        pairs.foreach(pair => {
          val (k, (count, avg, p90)) = pair
          redisClient.hset("current", k, "" + count + "," + avg + "," + p90)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}