package com.github.mjakubowski84.parquet4s

object ScalaKafkaCompat {
  object kafka {

    type CommitterSettings = org.apache.pekko.kafka.CommitterSettings
    def CommitterSettings = org.apache.pekko.kafka.CommitterSettings

    val ConsumerMessage = org.apache.pekko.kafka.ConsumerMessage

    type ConsumerSettings[K, V] = org.apache.pekko.kafka.ConsumerSettings[K, V]
    def ConsumerSettings = org.apache.pekko.kafka.ConsumerSettings

    def Subscriptions = org.apache.pekko.kafka.Subscriptions

    type Subscription = org.apache.pekko.kafka.Subscription

    object scaladsl {

      val Consumer = org.apache.pekko.kafka.scaladsl.Consumer

      def Committer = org.apache.pekko.kafka.scaladsl.Committer

    }
  }
}
