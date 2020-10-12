package com.github.mjakubowski84.parquet4s.fs2

import java.nio.file.Paths
import java.sql.Timestamp
import java.util.UUID

import cats.data.State
import cats.effect.{Blocker, ExitCode, IO, IOApp}
import com.github.mjakubowski84.parquet4s.ParquetWriter
import com.github.mjakubowski84.parquet4s.parquet._
import fs2.io.file.tempDirectoryStream
import fs2.kafka._
import fs2.{INothing, Pipe, Stream}
import net.manub.embeddedkafka.{EmbeddedK, EmbeddedKafka}
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.concurrent.duration._
import scala.util.Random

object IndefiniteFS2App extends IOApp {

  private type KafkaRecord = CommittableConsumerRecord[IO, String, String]

  private case class Data(
                           year: String,
                           month: String,
                           day: String,
                           timestamp: Timestamp,
                           word: String
                         )

  private sealed trait Fluctuation {
    def delay: FiniteDuration
  }
  private case class Up(delay: FiniteDuration) extends Fluctuation
  private case class Down(delay: FiniteDuration) extends Fluctuation

  private val TmpPath = Paths.get(sys.props("java.io.tmpdir"))
  private val MaxNumberOfRecordPerFile = 128
  private val MaxDurationOfFileWrite = 10.seconds
  private val WriterOptions = ParquetWriter.Options(compressionCodecName = CompressionCodecName.SNAPPY)
  private val MinDelay = 1.milli
  private val MaxDelay = 500.millis
  private val StartDelay = 100.millis
  private val Topic = "topic"
  private val Words = Seq("Example", "how", "to", "setup", "indefinite", "stream", "with", "Parquet", "writer")

  private def nextWord(): String = Words(Random.nextInt(Words.size - 1))

  private val fluctuate: State[Fluctuation, Unit] = State[Fluctuation, Unit] { fluctuation =>
    val rate = Random.nextFloat() / 10.0f
    val step = (fluctuation.delay.toMillis * rate).millis
    val nextFluctuation = fluctuation match {
      case Up(delay) if delay + step < MaxDelay =>
        Up(delay + step)
      case Up(delay) =>
        Down(delay - step)
      case Down(delay) if delay - step > MinDelay =>
        Down(delay - step)
      case Down(delay) =>
        Up(delay + step)
    }
    (nextFluctuation, ())
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val stream = for {
      blocker <- Stream.resource(Blocker[IO])
      writePath <- tempDirectoryStream[IO](blocker, dir = TmpPath)
      kafkaPort <- Stream
        .bracket(blocker.delay[IO, EmbeddedK](EmbeddedKafka.start()))(_ => blocker.delay[IO, Unit](EmbeddedKafka.stop()))
        .map(_.config.kafkaPort)
      producerSettings = ProducerSettings[IO, String, String]
        .withBootstrapServers(s"localhost:$kafkaPort")
      consumerSettings = ConsumerSettings[IO, String, String]
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers(s"localhost:$kafkaPort")
        .withGroupId("group")
      _ <- Stream(
        producer(producerSettings),
        consumer(blocker, consumerSettings, writePath.toString)
      ).parJoin(maxOpen = 2)
    } yield ()

    stream.compile.drain.as(ExitCode.Success)
  }

  private def producer(producerSettings: ProducerSettings[IO, String, String]): Stream[IO, INothing] =
    Stream
      .iterate(fluctuate.runS(Down(StartDelay)).value)(fluctuation => fluctuate.runS(fluctuation).value)
      .flatMap(fluctuation => Stream.sleep_(fluctuation.delay) ++ Stream.emit(nextWord()))
      .map(word => ProducerRecord(topic = Topic, key = UUID.randomUUID().toString, value = word))
      .map(ProducerRecords.one[String, String])
      .through(produce(producerSettings))
      .drain

  private def consumer(blocker: Blocker,
                       consumerSettings: ConsumerSettings[IO, String, String],
                       writePath: String): Stream[IO, INothing] =
    consumerStream[IO]
      .using(consumerSettings)
      .evalTap(_.subscribeTo(Topic))
      .flatMap(_.stream)
      .through(write(blocker, writePath))
      .map(_.offset)
      .through(commitBatchWithin(MaxNumberOfRecordPerFile, MaxDurationOfFileWrite))
      .drain

  private def write(blocker: Blocker, path: String): Pipe[IO, KafkaRecord, KafkaRecord] =
    viaParquet[IO, KafkaRecord]
      .options(WriterOptions)
      .maxCount(MaxNumberOfRecordPerFile)
      .maxDuration(MaxDurationOfFileWrite)
      .preWriteTransformation[Data] { kafkaRecord =>
        kafkaRecord.record.timestamp.createTime.map(l => new Timestamp(l)).fold[Stream[IO, Data]](Stream.empty) { timestamp =>
          val dateTime = timestamp.toLocalDateTime
          Stream.emit(Data(
            year = dateTime.getYear.toString,
            month = dateTime.getMonth.getValue.toString,
            day = dateTime.getDayOfMonth.toString,
            timestamp = timestamp,
            word = kafkaRecord.record.value
          )).evalTap(data => IO(println(data)))
        }
      }
      .partitionBy("year", "month", "day")
      .write(blocker, path)

}
