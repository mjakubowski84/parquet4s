package com.github.mjakubowski84.parquet4s

import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerMessage, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Source
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.duration.Duration

object MessageSource {

  type Message = ConsumerMessage.CommittableMessage[String, String]

}

trait MessageSource {

  this: Akka with Kafka =>

  import MessageSource._

  private val consumerSettings = ConsumerSettings(system, new StringDeserializer(), new StringDeserializer())
    .withBootstrapServers(kafkaAddress)
    .withGroupId(groupId)
    .withStopTimeout(Duration.Zero)
  private val subscription = Subscriptions.topics(topic)

  lazy val messageSource: Source[Message, Consumer.Control] = Consumer.committableSource(consumerSettings, subscription)

}
