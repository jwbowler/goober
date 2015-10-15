# Goober

A project for [Insight Data Engineering](http://insightdataengineering.com), September 2015.

The web app code is in the [goober-web](http://github.com/jwbowler/goober-web) repository.

## Scenario

The year is 2020, and Goober Inc. operates fleets of self-driving taxicabs in cities and towns across the world.

These driverless cars are directed around by a centralized computer system, and the data scientists at Goober want to control these cars optimally. The goal is to minimize wait times, and make sure that no geographic areas are underserved.

To explore the problem, data scientists need to know about user quality of service, past and present, at all locations. This project is an analytics pipeline designed to serve that data.

## Design

### Data generation and ingestion

The input data is derived from [NYC taxicab data](http://www.andresmh.com/nyctaxitrips/). The scripts in [producer/create-replay-file](producer/create-replay-file) take CSV files of real data and convert them to CSV files of fake self-driving-taxicab data used in this pipeline.

Each line of real data, representing a real taxicab ride, gets a fake "wait time" - a random number representing how long the user waited between requesting the ride and getting picked up. Then, each line of real data is used to generate a sequence of fake messages:
* One "ride request" message, representing a user taking out his phone and calling a Goober cab.
* A series of "ETA update" messages, one every two seconds, as a car drives towards a user and uses GPS to continuously recalculate its ETA.
* One "pickup" message, representing a user entering the cab.

These fake messages are sorted by timestamp and collected into a single replay file that a Python script can stream to [Kafka](https://kafka.apache.org/).

A single Kafka topic, split into 2 partitions, queues all messages with 24-hour retention.

### Batch and stream processing

A periodic [Camus](http://docs.confluent.io/1.0/camus/docs/intro.html) job consumes messages from Kafka and stores them in HDFS on a 4-node Hadoop cluster, sorted by timestamp and split into files per hour.

[BatchBin.scala](/src/main/scala/com/goober/BatchBin.scala) is a [Spark](http://spark.apache.org/) job that takes these historical messages from HDFS and builds a picture of a typical day. (Simplifying assumption: every day is a [Mon]day, with the same expected traffic patterns.) This "typical day" is a 2D lookup table, where rows are time-of-day intervals and columns are location zones. Every historical ride will fall in a range of buckets, representing the time that the user spent waiting for the ride to pull up. Each cell in this table is aggregated into summary statistics: number of outstanding rides, average wait time, and 90th-percentile wait time.

[StreamBin.scala](/src/main/scala/com/goober/StreamBin.scala) is similar in function to [BatchBin.scala](/src/main/scala/com/goober/BatchBin.scala), but written in [Spark Streaming](http://spark.apache.org/streaming/). This process consumes from Kafka in real time, and builds a picture of the Goober system's current performance. This process outputs a 1D lookup table, where currently-outstanding ride requests are bucketed by location and aggregated into the summary statistics described above.

### Serving layer

This pipeline uses [Redis](http://redis.io) as a datastore. The batch process writes a (location -> statistics) hash to a key for each time interval. The stream process writes a (location -> statistics) hash to a "current" key, and also publishes the last-processed timestamp to a topic.

The [web app](http://github.com/jwbowler/goober-web) reads the "current" per-location statistics, checks the timestamp, and then reads the historical statistics at the appropriate time interval. By comparing these two sets of statistics, the frontend can display whether the current quality-of-service at any given location is better or worse than it usually is at this time of day.


