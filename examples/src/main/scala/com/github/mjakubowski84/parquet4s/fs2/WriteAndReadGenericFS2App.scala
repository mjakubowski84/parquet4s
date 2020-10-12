package com.github.mjakubowski84.parquet4s.fs2

import java.nio.file.Paths
import java.time.{LocalDate, ZoneOffset}
import java.util.TimeZone

import cats.Show
import cats.effect.{Blocker, ExitCode, IO, IOApp}
import com.github.mjakubowski84.parquet4s.parquet._
import com.github.mjakubowski84.parquet4s.{RowParquetRecord, ValueCodecConfiguration}
import fs2.Stream
import fs2.io.file.tempDirectoryStream
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{BINARY, INT32, INT64}
import org.apache.parquet.schema.Type.Repetition.{OPTIONAL, REQUIRED}
import org.apache.parquet.schema.{MessageType, OriginalType, Types}

object WriteAndReadGenericFS2App extends IOApp {

  private val ID = "id"
  private val Name = "name"
  private val Birthday = "birthday"
  private val SchemaName = "user_schema"

  implicit val schema: MessageType = Types.buildMessage()
    .addField(Types.primitive(INT64, REQUIRED).as(OriginalType.INT_64).named(ID))
    .addField(Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named(Name))
    .addField(Types.primitive(INT32, OPTIONAL).as(OriginalType.DATE).named(Birthday))
    .named(SchemaName)

  private implicit val showRecords: Show[RowParquetRecord] = Show.fromToString

  private val TmpPath = Paths.get(sys.props("java.io.tmpdir"))

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


  override def run(args: List[String]): IO[ExitCode] = {
    val stream = for {
      blocker <- Stream.resource(Blocker[IO])
      path <- tempDirectoryStream[IO](blocker, dir = TmpPath)
      _ <- Stream.iterable[IO, RowParquetRecord](users)
        .through(writeSingleFile(blocker, path.resolve("data.parquet").toString))
        .append(fromParquet[IO, RowParquetRecord].read(blocker, path.toString).showLinesStdOut.drain)
    } yield ()

    stream.compile.drain.as(ExitCode.Success)
  }
}
