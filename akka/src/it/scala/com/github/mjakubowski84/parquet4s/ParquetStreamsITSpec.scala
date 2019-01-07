package com.github.mjakubowski84.parquet4s

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Random

object ParquetStreamsITSpec {
  case class Data(i: Long, s: String)
}

class ParquetStreamsITSpec extends AsyncFlatSpec
  with Matchers
  with SparkHelper {

  import ParquetRecordDecoder._
  import ParquetStreamsITSpec._

  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()

  val count = 10000

  val dict: Seq[String] = Vector("a", "b", "c", "d")

  override def beforeAll(): Unit = {
    super.beforeAll()
    clearTemp()
  }

  "ParquetStreams" should "read Parquet file correctly" in {

    import sparkSession.implicits._

    val data = Stream
      .continually(Data(Random.nextLong(), dict(Random.nextInt(4))))
      .take(count)

    data
      .toDS()
      .write.parquet(tempPathString)

    ParquetStreams.fromParquet[Data](tempPathString).runWith(Sink.seq).map { result =>
      result should have size count
      result should contain theSameElementsAs data
    }
  }

  override def afterAll(): Unit = {
    Await.ready(system.terminate(), Duration.Inf)
    super.afterAll()
  }

}
