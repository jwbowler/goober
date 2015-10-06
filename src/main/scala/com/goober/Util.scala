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

  def timeToBucket(time: Date): Interval = {
    val seconds = time.getSeconds + 60*(time.getMinutes + 60*time.getHours)
    val out = seconds / secondsPeriod
    assert(out >= 0 && out < numTimeBuckets)

    out
  }

}
