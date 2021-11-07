---
layout: docs
title: Introduction
permalink: docs/
---

This page is a work in progress. It is dedicated to the latest release candidate version of Parquet4s.
For a documentation of stable version 1.x of the library please refer to [Readme](https://github.com/mjakubowski84/Parquet4s).

# Introduction

Parquet4s is a simple I/O for [Parquet](https://parquet.apache.org/). Allows you to easily read and write Parquet files in [Scala](https://www.scala-lang.org/).

Use just a Scala case class to define the schema of your data. No need to use Avro, Protobuf, Thrift or other data serialisation systems. You can use generic records if you don't want to use the case class, too.

Compatible with files generated with [Apache Spark](https://spark.apache.org/). However, unlike in Spark, you do not have to start a cluster to perform I/O operations.

Based on official [Parquet library](https://github.com/apache/parquet-mr), [Hadoop Client](https://github.com/apache/hadoop) and [Shapeless](https://github.com/milessabin/shapeless) (Shapeless is not in use in a version for Scala 3).

As it is based on Hadoop Client then you can connect to any Hadoop-compatible storage like AWS S3 or Google Cloud Storage. 

Integrations for [Akka Streams](https://doc.akka.io/docs/akka/current/stream/index.html) and [FS2](https://fs2.io/).

Released for Scala 2.12.x, 2.13.x and 3.0.x.
Akka module is available for Scala 2.12.x and 2.13.x.
