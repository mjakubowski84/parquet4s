package com.github.mjakubowski84.parquet4s
import akka.Done
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Keep

import scala.concurrent.Await
import scala.concurrent.duration._

object ExampleApp
  extends App
    with Logger
    with Akka
    with Kafka
    with RandomDataProducer
    with MessageSource
    with MessageSink {


  startKafka()
  startDataProducer()

  logger.info(s"Starting stream that reads messages from Kafka and writes them to $baseWritePath...")
  val streamControl: Consumer.DrainingControl[Done] = messageSource
    .toMat(messageSink)(Keep.both)
    .mapMaterializedValue(Consumer.DrainingControl.apply)
    .run()

  def stopStream(): Unit = {
    logger.info("Stopping stream...")
    Await.ready(streamControl.drainAndShutdown(), 1.second)
  }

  sys.addShutdownHook {
    stopDataProducer()
    stopStream()
    stopKafka()
    stopAkka()
    logger.info("Exiting...")
  } 
}
