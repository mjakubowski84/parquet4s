package com.github.mjakubowski84.parquet4s.core

import com.github.mjakubowski84.parquet4s._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{BINARY, INT32, INT64}
import org.apache.parquet.schema.Type.Repetition.{OPTIONAL, REQUIRED}
import org.apache.parquet.schema.{LogicalTypeAnnotation, MessageType, Types}

import java.nio.file.Files
import java.time.LocalDate

object WriteAndReadGenericApp extends App {

  val ID = "id"
  val Name = "name"
  val Birthday = "birthday"
  val SchemaName = "user_schema"

  val path = Path(Files.createTempDirectory("example"))
  val vcc = ValueCodecConfiguration.Default

  val users = List(
    (1L, "Alice", LocalDate.of(2000, 1, 1)),
    (2L, "Bob", LocalDate.of(1980, 2, 28)),
    (3L, "Cecilia", LocalDate.of(1977, 3, 15))
  ).map { case (id, name, birthday) =>
    RowParquetRecord.emptyWithSchema(ID, Name, Birthday)
      .updated(ID, id, vcc)
      .updated(Name, name, vcc)
      .updated(Birthday, birthday, vcc)
  }

  // write
  implicit val schema: MessageType = Types.buildMessage()
    .addField(Types.primitive(INT64, REQUIRED).as(LogicalTypeAnnotation.intType(64, true)).named(ID))
    .addField(Types.primitive(BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named(Name))
    .addField(Types.primitive(INT32, OPTIONAL).as(LogicalTypeAnnotation.dateType()).named(Birthday))
    .named(SchemaName)

  ParquetWriter.writeAndClose(path.append("users.parquet"), users)

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
