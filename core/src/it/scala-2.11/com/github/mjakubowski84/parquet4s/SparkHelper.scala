package com.github.mjakubowski84.parquet4s

import java.nio.file.{Path, Paths}

import com.google.common.io.Files
import com.github.mjakubowski84.parquet4s.TestUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.reflect.runtime.universe.TypeTag

trait SparkHelper extends BeforeAndAfterAll with TestUtils {

  this: Suite =>

  private var sparkStarted = false

  lazy val sparkSession: SparkSession = {
    sparkStarted = true
    SparkSession.builder.master("local[2]").appName(getClass.getSimpleName).getOrCreate
  }

  override def afterAll() {
    super.afterAll()
    if (sparkStarted) sparkSession.stop()
  }

  def writeToTemp[T <: Product : TypeTag](data: Seq[T]): Unit = {
    import sparkSession.implicits._
    data.toDS().write.parquet(tempPathString)
  }

  def readFromTemp[T <: Product : TypeTag]: Seq[T] = {
    import sparkSession.implicits._
    sparkSession.read.parquet(tempPathString).as[T].collect().toSeq
  }

}
