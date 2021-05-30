package com.github.mjakubowski84.parquet4s.akka.indefinite

import io.github.embeddedkafka.EmbeddedKafka

trait Kafka {

  this: Logger =>

  private lazy val broker = {
    logger.info("Starting Kafka...")
    EmbeddedKafka.start()
  }

  lazy val kafkaAddress = s"localhost:${broker.config.kafkaPort}"
  val topic = "exampleTopic"
  val groupId = "exampleGroupId"

  def sendKafkaMessage(message: String): Unit = EmbeddedKafka.publishStringMessageToKafka(topic, message)

  def startKafka(): Unit = {
    broker
    ()
  }

  def stopKafka(): Unit = {
    logger.info("Stopping Kafka...")
    EmbeddedKafka.stop()
  }

}
