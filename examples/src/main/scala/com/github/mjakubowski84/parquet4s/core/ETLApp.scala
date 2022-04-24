package com.github.mjakubowski84.parquet4s.core

import com.github.mjakubowski84.parquet4s.*

import java.nio.file.Files

object ETLApp extends App {

  case class Owner(id: Long, name: String)
  case class Pet(id: Long, name: String, ownerId: Long)
  case class PetOwner(id: Long, name: String, petId: Long, petName: String)

  val path       = Path(Files.createTempDirectory("example"))
  val ownerPath  = path.append("owners.parquet")
  val petsPath   = path.append("pets.parquet")
  val outputPath = path.append("output.parquet")

  val owners = List(
    Owner(1L, "Alice"),
    Owner(2L, "Bob"),
    Owner(3L, "Cecilia")
  )
  val pets = List(
    Pet(1L, "Rex", 2L),
    Pet(2L, "Felix", 3L),
    Pet(3L, "Molly", 3L),
    Pet(4L, "Sunshine", 4L)
  )

  // write
  ParquetWriter.of[Owner].writeAndClose(ownerPath, owners)
  ParquetWriter.of[Pet].writeAndClose(petsPath, pets)

  // define 1st dataset
  val readOwners = ParquetReader
    .projectedGeneric(
      Col("id").as[Long],
      Col("name").as[String]
    )
    .read(ownerPath)

  // define 2nd dataset
  val readPets = ParquetReader
    .projectedGeneric(
      Col("id").as[Long].alias("petId"),
      Col("name").as[String].alias("petName"),
      Col("ownerId").as[Long]
    )
    .read(petsPath)

  readOwners
    .innerJoin(right = readPets, onLeft = Col("id"), onRight = Col("ownerId")) // define join operation
    .as[PetOwner] // set typed schema and codecs
    .writeAndClose(outputPath) // execute all operations defined above and write results to disk

  // take note that all operations defined above writeAndClose are lazy and are not executed
  // until writeAndClose is called

}
