package com.github.mjakubowski84.parquet4s.akka.indefinite

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.stream.scaladsl.Keep

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
  val streamControl: DrainingControl[Done] = messageSource
    .toMat(messageSink)(Keep.both)
    .mapMaterializedValue(DrainingControl.apply[Done])
    .run()

  coordinatedShutdown.addTask(CoordinatedShutdown.PhaseServiceStop, "Stopping stream") { () =>
    logger.info("Stopping stream...")
    streamControl.drainAndShutdown()
  }

}
