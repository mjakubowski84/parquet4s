package com.github.mjakubowski84.parquet4s.core

import java.time.{LocalDate, ZoneOffset}
import java.util.TimeZone

import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter, RowParquetRecord, ValueCodecConfiguration}
import com.google.common.io.Files
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{BINARY, INT32, INT64}
import org.apache.parquet.schema.Type.Repetition.{OPTIONAL, REQUIRED}
import org.apache.parquet.schema.{MessageType, OriginalType, Types}

object WriteAndReadGenericApp extends App {

  import RowParquetRecord._

  val ID = "id"
  val Name = "name"
  val Birthday = "birthday"
  val Schema = "user_schema"

  val path = Files.createTempDir().getAbsolutePath
  val vcc = ValueCodecConfiguration(TimeZone.getTimeZone(ZoneOffset.UTC))

  val users = List(
    (1L, "Alice", LocalDate.of(2000, 1, 1)),
    (2L, "Bob", LocalDate.of(1980, 2, 28)),
    (3L, "Cecilia", LocalDate.of(1977, 3, 15))
  ).map { case (id, name, birthday) =>
    RowParquetRecord.empty
      .add(ID, id, vcc)
      .add(Name, name, vcc)
      .add(Birthday, birthday, vcc)
  }

  // write
  implicit val schema: MessageType = Types.buildMessage()
    .addField(Types.primitive(INT64, REQUIRED).as(OriginalType.INT_64).named(ID))
    .addField(Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named(Name))
    .addField(Types.primitive(INT32, OPTIONAL).as(OriginalType.DATE).named(Birthday))
    .named(Schema)

  ParquetWriter.writeAndClose(s"$path/users.parquet", users)

  //read
  val readData = ParquetReader.read[RowParquetRecord](path)
  try {
    readData.foreach { record =>
      val id = record.get[Long](ID, vcc)
      val name = record.get[String](Name, vcc)
      val birthday = record.get[LocalDate](Birthday, vcc)
      println(s"User[$ID=$id,$Name=$name,$Birthday=$birthday]")
    }
  } finally readData.close()

}
