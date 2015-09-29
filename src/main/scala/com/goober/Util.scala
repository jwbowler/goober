package com.goober

import java.util.Date

object Util {

  type Loc = Int
  type Interval = Int
  type Eta = Double

  type Uid = Long

  type LocEtaPair = (Option[Loc], Option[Eta], String)
  type UserRecord = (Uid, LocEtaPair)
  type UidEtaPair = (Uid, Eta)
  type EtaList = Seq[Eta]

  def locationToBucket(lng: Double, lat: Double): Option[Loc] = {
    val numBucketsX = 10
    val numBucketsY = 10

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

      Some(row * numBucketsY + col)
    }
  }

  def timeToBucket(time: Date): Option[Interval] = {
    val secondsPeriod = 1
    val numBuckets = 86400 / secondsPeriod

    val seconds = time.getSeconds + 60*(time.getMinutes + 60*time.getHours)

    val out = seconds / secondsPeriod

    if (out < 0 || out >= numBuckets) {
      None
    } else {
      Some(out)
    }
  }

}
