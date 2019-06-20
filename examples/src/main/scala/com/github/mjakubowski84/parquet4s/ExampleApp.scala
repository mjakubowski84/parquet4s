package com.github.mjakubowski84.parquet4s
import java.nio.file.Paths

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch}
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Flow, Keep}
import akka.stream.{ActorMaterializer, Materializer}
import com.google.common.io.Files
import net.manub.embeddedkafka.{Consumers, EmbeddedKafka}
import org.apache.hadoop.fs.Path
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.slf4j.LoggerFactory
import java.sql.Timestamp

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

object ExampleApp extends App with Consumers {

  case class Message(timestamp: Timestamp, word: String)
  val words = Seq("Example", "how", "to", "setup", "indefinite", "stream", "with", "Parquet", "writer")

  val logger = LoggerFactory.getLogger(this.getClass)

  val topic = "myTopic"
  val path = Paths.get(Files.createTempDir().getAbsolutePath, "messages").toString
  val writerOptions = ParquetWriter.Options(compressionCodecName = CompressionCodecName.SNAPPY)

  logger.info("Starting Kafka...")
  val broker = EmbeddedKafka.start()

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()
  import system.dispatcher

  logger.info("Starting scheduler that sends messages to Kafka...")
  val scheduler = system.scheduler.schedule(1.second, 50.millis) {
    EmbeddedKafka.publishStringMessageToKafka(topic, words(Random.nextInt(words.size - 1)))
  }
  
  val consumerSettings = ConsumerSettings(system, new StringDeserializer(), new StringDeserializer())
    .withBootstrapServers(s"localhost:${broker.config.kafkaPort}")
    .withGroupId("myGroup")
    .withStopTimeout(Duration.Zero)
  val subscription = Subscriptions.topics(topic)
  val messageSource = Consumer.committableSource(consumerSettings, subscription)

  val committerSink = Flow.apply[Seq[CommittableMessage[String, String]]].map { messages =>
    CommittableOffsetBatch(messages.map(_.committableOffset))
  }.toMat(Committer.sink(CommitterSettings(system)))(Keep.right)

  val parquetSink = IndefiniteStreamParquetSink(
    path = new Path(path),
    maxChunkSize = 128,
    chunkWriteTimeWindow = 10.seconds,
    preWriteTransformation = { cm: CommittableMessage[String, String] => 
      Message(
        timestamp = new Timestamp(cm.record.timestamp()),
        word = cm.record.value()
      ) 
    },
    postWriteSink = committerSink,
    options = writerOptions
  )

  logger.info(s"Starting consumer that reads messages from Kafka and writes them to $path...")
  val control: Consumer.DrainingControl[Done] = messageSource
    .toMat(parquetSink)(Keep.both)
    .mapMaterializedValue(Consumer.DrainingControl.apply)
    .run()
   
  sys.addShutdownHook {
    logger.info("Stopping scheduler...")
    scheduler.cancel()
    logger.info("Stopping consumer...")
    Await.ready(control.drainAndShutdown(), 1.second)
    logger.info("Stopping Kafka")
    EmbeddedKafka.stop()
    logger.info("Stopping Akka...")
    Await.ready(system.terminate(), 1.second)
    logger.info("Exiting...")
  } 
}
