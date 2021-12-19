package com.github.mjakubowski84.parquet4s.core

import com.github.mjakubowski84.parquet4s.*

import java.nio.file.Files
import java.time.LocalDate

object ColumnProjectionAndDataConcatenationApp extends App {

  val ID        = "id"
  val Name      = "name"
  val FirstName = "firstName"
  val Birthday  = "birthday"

  case class User1(id: Long, name: String, birthday: LocalDate)
  case class User2(id: Int, firstName: String, lastName: String)
  case class UserName(id: Long, name: String)

  val path  = Path(Files.createTempDirectory("example"))
  val path1 = path.append("users1.parquet")
  val path2 = path.append("users2.parquet")

  val vcc = ValueCodecConfiguration.Default

  val users1 = List(
    User1(1L, "Alice", LocalDate.of(2000, 1, 1)),
    User1(2L, "Bob", LocalDate.of(1980, 2, 28)),
    User1(3L, "Cecilia", LocalDate.of(1977, 3, 15))
  )
  val users2 = List(
    User2(4, "Derek", "Smith"),
    User2(5, "Emilia", "Doe"),
    User2(6, "Fred", "Johnson")
  )

  // write
  ParquetWriter.of[User1].writeAndClose(path1, users1)
  ParquetWriter.of[User2].writeAndClose(path2, users2)

  // define 1st dataset
  val readUsers1 = ParquetReader
    .projectedGeneric(
      Col(ID).as[Long],
      Col(Name).as[String]
    )
    .read(path1)
    .as[UserName]

  // define 2nd dataset
  val readUsers2 = ParquetReader
    .projectedGeneric(
      Col(ID).as[Int],
      Col(FirstName).as[String].alias(Name)
    )
    .read(path2)
    .as[UserName]

  // define concatenation of datasets
  val readAllUserNames = readUsers1.concat(readUsers2)

  // execute
  try readAllUserNames.foreach(println)
  finally readAllUserNames.close()

}
