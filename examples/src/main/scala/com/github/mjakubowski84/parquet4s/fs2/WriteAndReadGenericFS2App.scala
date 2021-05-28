package com.github.mjakubowski84.parquet4s.fs2

import cats.Show
import cats.effect.{IO, IOApp}
import com.github.mjakubowski84.parquet4s.parquet._
import com.github.mjakubowski84.parquet4s.{Path, RowParquetRecord, ValueCodecConfiguration}
import fs2.Stream
import fs2.io.file.Files
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{BINARY, INT32, INT64}
import org.apache.parquet.schema.Type.Repetition.{OPTIONAL, REQUIRED}
import org.apache.parquet.schema.{LogicalTypeAnnotation, MessageType, Types}

import java.time.LocalDate

object WriteAndReadGenericFS2App extends IOApp.Simple {

  private val ID = "id"
  private val Name = "name"
  private val Birthday = "birthday"
  private val SchemaName = "user_schema"

  implicit val schema: MessageType = Types.buildMessage()
    .addField(Types.primitive(INT64, REQUIRED).as(LogicalTypeAnnotation.intType(64, true)).named(ID))
    .addField(Types.primitive(BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named(Name))
    .addField(Types.primitive(INT32, OPTIONAL).as(LogicalTypeAnnotation.dateType()).named(Birthday))
    .named(SchemaName)

  private implicit val showRecords: Show[RowParquetRecord] = Show.fromToString

  private val vcc = ValueCodecConfiguration.Default

  private val users = List(
    (1L, "Alice", LocalDate.of(2000, 1, 1)),
    (2L, "Bob", LocalDate.of(1980, 2, 28)),
    (3L, "Cecilia", LocalDate.of(1977, 3, 15))
  ).map { case (id, name, birthday) =>
    RowParquetRecord.emptyWithSchema(ID, Name, Birthday)
      .updated(ID, id, vcc)
      .updated(Name, name, vcc)
      .updated(Birthday, birthday, vcc)
  }


  override def run: IO[Unit] = {
    val stream = for {
      path <- Stream.resource(Files[IO].tempDirectory()).map(Path.apply)
      _ <- Stream.iterable[IO, RowParquetRecord](users)
        .through(writeSingleFile(path.append("data.parquet")))
        .append(fromParquet[IO, RowParquetRecord].read(path).printlns.drain)
    } yield ()

    stream.compile.drain
  }
}
