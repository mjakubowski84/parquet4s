package com.github.mjakubowski84.parquet4s.akkaPekko.indefinite

import com.github.mjakubowski84.parquet4s.ScalaCompat.Done
import com.github.mjakubowski84.parquet4s.ScalaCompat.actor.CoordinatedShutdown
import com.github.mjakubowski84.parquet4s.ScalaKafkaCompat.kafka.scaladsl.Consumer.DrainingControl
import com.github.mjakubowski84.parquet4s.ScalaCompat.stream.scaladsl.Keep

object ExampleApp
    extends App
    with Logger
    with AkkaPekko
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
