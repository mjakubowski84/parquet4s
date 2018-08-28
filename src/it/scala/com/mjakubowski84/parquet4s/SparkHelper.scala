package com.mjakubowski84.parquet4s

import java.nio.file.{Path, Paths}

import com.google.common.io.Files
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.reflect.runtime.universe.TypeTag

trait SparkHelper extends BeforeAndAfterAll  {

  this: Suite =>

  lazy val sparkSession: SparkSession =
    SparkSession.builder.master("local[2]").appName(getClass.getSimpleName).getOrCreate

  private val tempPath: Path = Paths.get(Files.createTempDir().getAbsolutePath, "sparkOutputPath")

  val tempPathString: String = tempPath.toString

  override def afterAll() {
    super.afterAll()
    sparkSession.stop()
  }

  def writeToTemp[T <: Product : TypeTag](data: Seq[T]): Unit = {
    import sparkSession.implicits._
    data.toDS().write.parquet(tempPathString)
  }

  def clearTemp(): Unit = {
    val tempDir = tempPath.toFile
    Option(tempDir.listFiles()).foreach(_.foreach(_.delete()))
    tempDir.delete()
  }

}
