package com.github.mjakubowski84.parquet4s
import akka.actor.Cancellable

import scala.concurrent.duration._
import scala.util.Random


object RandomDataProducer {

  private val words = Seq("Example", "how", "to", "setup", "indefinite", "stream", "with", "Parquet", "writer")

}

trait RandomDataProducer {

  this: Akka with Logger with Kafka =>

  import RandomDataProducer._

  private def nextWord: String = words(Random.nextInt(words.size - 1))

  private lazy val scheduler: Cancellable = {
    logger.info("Starting scheduler that sends messages to Kafka...")
    system.scheduler.schedule(initialDelay = 1.second, interval = 50.millis) {
      sendKafkaMessage(nextWord)
    }
  }

  def startDataProducer(): Unit = scheduler

  def stopDataProducer(): Unit = {
    logger.info("Stopping scheduler...")
    scheduler.cancel()
  }

}
