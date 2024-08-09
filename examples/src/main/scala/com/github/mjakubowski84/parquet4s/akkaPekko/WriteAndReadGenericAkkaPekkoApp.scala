package com.github.mjakubowski84.parquet4s.akkaPekko

import com.github.mjakubowski84.parquet4s.ScalaCompat.actor.ActorSystem
import com.github.mjakubowski84.parquet4s.ScalaCompat.stream.scaladsl.{Sink, Source}
import com.github.mjakubowski84.parquet4s.{ParquetStreams, Path, RowParquetRecord, ValueCodecConfiguration}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{BINARY, INT32, INT64}
import org.apache.parquet.schema.Type.Repetition.{OPTIONAL, REQUIRED}
import org.apache.parquet.schema.{LogicalTypeAnnotation, MessageType, Types}

import java.nio.file.Files
import java.time.LocalDate

object WriteAndReadGenericAkkaPekkoApp extends App {

  val ID         = "id"
  val Name       = "name"
  val Birthday   = "birthday"
  val SchemaName = "user_schema"

  val Schema: MessageType = Types
    .buildMessage()
    .addField(Types.primitive(INT64, REQUIRED).as(LogicalTypeAnnotation.intType(64, true)).named(ID))
    .addField(Types.primitive(BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named(Name))
    .addField(Types.primitive(INT32, OPTIONAL).as(LogicalTypeAnnotation.dateType()).named(Birthday))
    .named(SchemaName)

  val Vcc = ValueCodecConfiguration.Default

  val users = List(
    (1L, "Alice", LocalDate.of(2000, 1, 1)),
    (2L, "Bob", LocalDate.of(1980, 2, 28)),
    (3L, "Cecilia", LocalDate.of(1977, 3, 15))
  ).map { case (id, name, birthday) =>
    RowParquetRecord
      .emptyWithSchema(ID, Name, Birthday)
      .updated(ID, id, Vcc)
      .updated(Name, name, Vcc)
      .updated(Birthday, birthday, Vcc)
  }

  val path = Path(Files.createTempDirectory("example"))

  implicit val system: ActorSystem = ActorSystem()
  import system.dispatcher

  val stream = for {
    // write
    _ <- Source(users).runWith(ParquetStreams.toParquetSingleFile.generic(Schema).write(path.append("data.parquet")))
    // read
    _ <- ParquetStreams.fromParquet.generic.read(path).runWith(Sink.foreach(println))
  } yield ()

  stream.andThen {
    // finish
    case _ => system.terminate()
  }
}
