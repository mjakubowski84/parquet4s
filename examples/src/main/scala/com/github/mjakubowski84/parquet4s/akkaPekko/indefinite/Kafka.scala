package com.github.mjakubowski84.parquet4s.akkaPekko.indefinite

import com.github.mjakubowski84.parquet4s.ScalaCompat.Done
import com.github.mjakubowski84.parquet4s.ScalaCompat.actor.CoordinatedShutdown
import io.github.embeddedkafka.EmbeddedKafka

import scala.concurrent.Future

trait Kafka {

  this: Logger & AkkaPekko =>

  private lazy val broker = {
    logger.info("Starting Kafka...")
    EmbeddedKafka.start()
  }

  lazy val kafkaAddress = s"localhost:${broker.config.kafkaPort}"
  val topic             = "exampleTopic"
  val groupId           = "exampleGroupId"

  def sendKafkaMessage(message: String): Unit = EmbeddedKafka.publishStringMessageToKafka(topic, message)

  def startKafka(): Unit = {
    broker
    coordinatedShutdown.addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "Stop kafka") { () =>
      Future {
        logger.info("Stopping Kafka...")
        EmbeddedKafka.stop()
        Done
      }
    }
  }

}
