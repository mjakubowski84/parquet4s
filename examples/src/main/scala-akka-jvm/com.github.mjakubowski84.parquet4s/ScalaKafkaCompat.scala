package com.github.mjakubowski84.parquet4s

object ScalaKafkaCompat {
  object kafka {

    type CommitterSettings = akka.kafka.CommitterSettings
    def CommitterSettings = akka.kafka.CommitterSettings

    val ConsumerMessage = akka.kafka.ConsumerMessage

    type ConsumerSettings[K, V] = akka.kafka.ConsumerSettings[K, V]
    def ConsumerSettings = akka.kafka.ConsumerSettings

    def Subscriptions = akka.kafka.Subscriptions

    type Subscription = akka.kafka.Subscription

    object scaladsl {

      val Consumer = akka.kafka.scaladsl.Consumer

      def Committer = akka.kafka.scaladsl.Committer

    }
  }
}
