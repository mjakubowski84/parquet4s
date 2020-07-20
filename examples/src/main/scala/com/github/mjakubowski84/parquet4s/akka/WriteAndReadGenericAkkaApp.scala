package com.github.mjakubowski84.parquet4s.akka

import java.time.{LocalDate, ZoneOffset}
import java.util.TimeZone

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import com.github.mjakubowski84.parquet4s.{ParquetStreams, RowParquetRecord, ValueCodecConfiguration}
import com.google.common.io.Files
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{BINARY, INT32, INT64}
import org.apache.parquet.schema.Type.Repetition.{OPTIONAL, REQUIRED}
import org.apache.parquet.schema.{MessageType, OriginalType, Types}

object WriteAndReadGenericAkkaApp extends App {

  private val ID = "id"
  private val Name = "name"
  private val Birthday = "birthday"
  private val SchemaName = "user_schema"

  implicit val schema: MessageType = Types.buildMessage()
    .addField(Types.primitive(INT64, REQUIRED).as(OriginalType.INT_64).named(ID))
    .addField(Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named(Name))
    .addField(Types.primitive(INT32, OPTIONAL).as(OriginalType.DATE).named(Birthday))
    .named(SchemaName)

  private val vcc = ValueCodecConfiguration(TimeZone.getTimeZone(ZoneOffset.UTC))

  private val users = List(
    (1L, "Alice", LocalDate.of(2000, 1, 1)),
    (2L, "Bob", LocalDate.of(1980, 2, 28)),
    (3L, "Cecilia", LocalDate.of(1977, 3, 15))
  ).map { case (id, name, birthday) =>
    RowParquetRecord.empty
      .add(ID, id, vcc)
      .add(Name, name, vcc)
      .add(Birthday, birthday, vcc)
  }

  val path = Files.createTempDir().getAbsolutePath

  implicit val system: ActorSystem = ActorSystem()
  import system.dispatcher

  for {
    // write
    _ <- Source(users).runWith(ParquetStreams.toParquetSingleFile(s"$path/data.parquet"))
    // read
    _ <- ParquetStreams.fromParquet[RowParquetRecord](path).runWith(Sink.foreach(println))
    // finish
    _ <- system.terminate()
  } yield ()

}
