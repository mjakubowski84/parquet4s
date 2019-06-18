package com.github.mjakubowski84.parquet4s
import net.manub.embeddedkafka.EmbeddedKafka
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.ActorMaterializer
import scala.concurrent.duration._
import net.manub.embeddedkafka.Consumers
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import net.manub.embeddedkafka.ConsumerExtensions._
import scala.collection.JavaConverters._
import akka.kafka.scaladsl.Consumer
import akka.kafka.ConsumerSettings
import org.apache.kafka.common.serialization.StringDeserializer
import akka.kafka.Subscriptions
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import scala.concurrent.Future
import akka.Done
import akka.stream.KillSwitches
import akka.stream.scaladsl.Keep
import scala.concurrent.Await
import java.nio.file.Paths
import com.google.common.io.Files
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.Path
import akka.kafka.scaladsl.Committer
import akka.kafka.ConsumerMessage.{CommittableOffset, CommittableMessage, Committable, CommittableOffsetBatch}
import akka.stream.scaladsl.Flow
import akka.kafka.CommitterSettings

object Example extends App with Consumers {

  case class Message(timestamp: java.sql.Timestamp)

  val logger = LoggerFactory.getLogger(this.getClass())

  val topic = "myTopic"
  val path = Paths.get(Files.createTempDir().getAbsolutePath, "messages").toString()
  val writerOptions = ParquetWriter.Options(compressionCodecName = CompressionCodecName.SNAPPY)

  logger.info("Starting Kafka...")
  val broker = EmbeddedKafka.start()

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()
  import system.dispatcher

  logger.info("Starting scheduler that sends messages to Kafka...")
  val scheduler = system.scheduler.schedule(1.second, 50.millis) {
    EmbeddedKafka.publishStringMessageToKafka(topic, "Message")
  }
  
  val consumerSettings = ConsumerSettings(system, new StringDeserializer(), new StringDeserializer())
    .withBootstrapServers(s"localhost:${broker.config.kafkaPort}")
    .withGroupId("myGroup")
  val subscription = Subscriptions.topics(topic)

  val (control, messageSource) = Consumer.committableSource(consumerSettings, subscription).preMaterialize()


  
  val parquetSink = IndefiniteStreamParquetSink[Message](
    path = new Path(path), 
    maxChunkSize = 128, 
    chunkWriteTimeWindow = 10.seconds,
    options = writerOptions
  )


  logger.info(s"Starting consumer that reads messages from Kafka and writes them to $path...")
  val (killSwitch, process) = messageSource
    .map { commitableRecord =>
      Message(new java.sql.Timestamp(commitableRecord.record.timestamp()))
    }
    .toMat(parquetSink)(Keep.both)
    .run()

  /*
  TODO:
  1. add to sink with .groupedWithin(128, 10.seconds) v
  2. add to sink callback when message is successfully writted
  1. add to sink function that is a file name factory
  */  
   
  sys.addShutdownHook {
    logger.info("Stopping scheduler")
    scheduler.cancel()
    logger.info("Stopping consumer...")
    Await.ready(process, 1.second)
    Await.ready(system.terminate(), 1.second)
    logger.info("Stopping Kafka")
    EmbeddedKafka.stop()
    logger.info("Exiting...")
  } 
}
