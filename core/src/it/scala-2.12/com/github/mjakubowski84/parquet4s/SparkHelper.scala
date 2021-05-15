package com.github.mjakubowski84.parquet4s

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

  def writeToTemp[T <: Product: TypeTag](data: Seq[T]): Unit = {
    import sparkSession.implicits._
    data.toDS().write.parquet(tempPath.toString)
  }

  def readFromTemp[T <: Product: TypeTag]: Seq[T] = {
    import sparkSession.implicits._
    sparkSession.read.parquet(tempPath.toString).as[T].collect().toSeq
  }

}
