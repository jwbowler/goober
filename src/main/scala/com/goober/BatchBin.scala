package com.goober

import java.text.SimpleDateFormat

import com.goober.Util._
import com.redis.RedisClient
import com.tdunning.math.stats.AVLTreeDigest
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object BatchBin {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = new SparkContext()

//    val df = sqlc.read.json("hdfs://ec2-52-89-161-16.us-west-2.compute.amazonaws.com:9000/camus/topics/m2/hourly/*/*/*/*/*")

    val text = sc.textFile("hdfs://ec2-52-25-69-74.us-west-2.compute.amazonaws.com:9000/monday-all.csv")

    // Tokenize messages
    val messages = text.map(_.split(","))

    // Filter out everything except "pickup" messages, which contain authoritative wait times for each requested ride.
    //
    // (The other message types, "request" and "ETA update", are useful in the streaming case where this final wait time
    // is unknown and can only be estimated from ETA updates.)
    //
    // (Also, make sure we're not reading the header line of a CSV file)
    val pickupMessages = messages.filter(words => {
      val rideId = words(1)
      val msgType = words(6)
      msgType == "PKP" && rideId != "ride_id"
    })

    // Bucket messages by time interval and location, and pull out "wait time" as the important value.
    //
    // Example: if Joe gets picked up at 00:43:30 in Neighborhood 17 after having waited for 189 seconds,
    // and if we bucket by minute (numBuckets == 24*60), we output:
    // (43, 17, 189)
    //
    // If we bucketed by second (numBuckets == 24*60*60), we would output:
    // (2610, 17, 189)
    val parsedMessages = pickupMessages.map(words => {
      val rideId = words(1).toLong
      val timestamp = words(2)
      val longitude = words(3).toDouble
      val latitude = words(4).toDouble
      val waitTime = words(5).toInt
      val msgType = words(6)

      val locBucket = locationToBucket(longitude.toDouble, latitude.toDouble)
      val timeBucket = timestampToBucket(timestamp)

      (timeBucket, locBucket, waitTime)
    })

    // Filter out messages with locations that fall outside the expected range
    val filteredMessages = parsedMessages.filter(tuple => {
      val (_, locBucket, _) = tuple
      locBucket.isDefined
    })
    .map(tuple => {
      val (timeBucket, locBucket, waitTime) = tuple
      (timeBucket, locBucket.get, waitTime)
    })

    // A single "pickup" message falls into a single time bucket. However, Joe might be waiting for a ride through
    // multiple consecutive time buckets, and we want to know his ETA in each.
    //
    // Continuing the example from above, (the bucket-by-minute case), Joe's pickup message...
    // (43, 17, 189)
    // ...would get flatMapped to...
    // (40, 17, 180),  (41, 17, 120),  (42, 17, 60)
    // The "189" is a total wait time, and the "180", "120", and "60" are ETA's at different instances in time.
    val splitEtas = filteredMessages.flatMap(tuple => {
      val (timeBucket, locBucket, waitTime) = tuple

      for (bucketDelta: Int <- List.range(1, waitTime/secondsPeriod + 1)) yield {
        val eta = bucketDelta * secondsPeriod

        // Keep all new time buckets in the [0, numTimeBuckets) range
        val newTimeBucket: Int = (timeBucket - bucketDelta) mod numTimeBuckets

        (newTimeBucket, locBucket, eta)
      }
    })

    // Key messages by (location-bucket, time-bucket), and group-by-key the ETAs of all requested rides in that time
    // and location.
    val etaLists = splitEtas.map(tuple => {
      val (timeBucket, locBucket, eta) = tuple
      val k = (timeBucket, locBucket)
      val v = Seq(eta.toDouble)
      (k, v)
    }).reduceByKey(_ ++ _)

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

    // Prepare to load hashes into Redis: switch from a ((k1, k2) -> v) structure to a (k1 -> (k2 -> v)) structure, and
    // stringify k2 and v.
    val groupedByLocation = stats.map(pair => {
      val (k, v) = pair
      val (count, avg, p90) = v
      val (timeBucket, locBucket) = k

      val valStr = "" + count + "," + avg + "," + p90
      (timeBucket, Map(locBucket.toString -> valStr))
    }).reduceByKey(_ ++ _)

    // Load into Redis, where each key is a timeBucket, and each value is a hash map from locationBuckets to summary
    // statistics.
    groupedByLocation.foreachPartition(part => {
      val redisClient = new RedisClient(redisHost, redisPort)
      part.foreach(pair => {
        val (k, v) = pair
        val redisKey = secondsPeriod + "b-" + k
        redisClient.hmset(redisKey, v)
      })
    })
  }
}
