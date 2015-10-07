package com.goober

import java.text.SimpleDateFormat
import java.util.Date

object Util {

  type Uid = Long
  type Loc = Int
  type Interval = Int
  type Eta = Double

  type MessageTuple1 = (MessageType.MessageType, Option[Loc], Option[Eta])

  type LocEtaPair = (Option[Loc], Option[Eta])
//  type UserRecord = (Uid, LocEtaPair)
//  type UidEtaPair = (Uid, Eta)
//  type EtaList = Seq[Eta]

  val redisHost = "john-redis.2wlafm.ng.0001.usw2.cache.amazonaws.com"
  val redisPort = 6379

  object MessageType extends Enumeration {
    type MessageType = Value
    val REQUEST, ETA_UPDATE, PICKUP = Value

    def fromMessageString(str: String) = str match {
      case "REQ" => REQUEST
      case "ETA" => ETA_UPDATE
      case "PKP" => PICKUP
    }
  }

  // create a 'mod' operator using scala's 'remainder' (%) operator
  implicit class Mod(val num: Int) extends AnyVal {
    def mod(divisor: Int) = ((num % divisor) + divisor) % divisor
  }

  def locationToBucket(lng: Double, lat: Double): Option[Loc] = {
    val numBucketsX = 20
    val numBucketsY = 20

    val minLng = -74.05
    val maxLng = -73.87
    val minLat = 40.68
    val maxLat = 40.89

    val scaledLng = (lng - minLng) / (maxLng - minLng)
    val scaledLat = (maxLat - lat) / (maxLat - minLat)

    if (scaledLng < 0 || scaledLng >= 1 || scaledLat < 0 || scaledLat >= 1) {
      None
    }

    else {
      val col = Math.floor(scaledLng * numBucketsX).toInt
      val row = Math.floor(scaledLat * numBucketsY).toInt

      Some(row * numBucketsY + col)
    }
  }

  val secondsPeriod = 60
  val numTimeBuckets = 86400 / secondsPeriod

  def timestampToBucket(timestamp: String): Interval = {
    val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timestamp)
    val seconds = date.getSeconds + 60*(date.getMinutes + 60*date.getHours)
    val out = seconds / secondsPeriod
    assert(out >= 0 && out < numTimeBuckets)

    out
  }

}
