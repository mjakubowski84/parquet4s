package com.github.mjakubowski84.parquet4s.s3

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.dimafeng.testcontainers.LocalStackV2Container
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.apache.hadoop.conf.Configuration
import scala.util.Using
import com.github.mjakubowski84.parquet4s.Path
import com.github.mjakubowski84.parquet4s.ParquetWriter
import com.github.mjakubowski84.parquet4s.ParquetReader

class S3ItSpec extends AnyFlatSpec with Matchers with TestContainerForAll {

  case class Data(i: Int, text: String)

  val bucket = "data"
  val data   = Seq(Data(1, "a"), Data(2, "b"))
  val path   = Path(s"s3a://$bucket/file.parquet")

  override val containerDef: LocalStackV2Container.Def =
    LocalStackV2Container.Def(
      tag      = "latest",
      services = Seq(Service.S3)
    )

  override def afterContainersStart(containers: LocalStackV2Container): Unit =
    containers.execInContainer("awslocal", "s3api", "create-bucket", "--bucket", bucket)

  "Parquet4s" should "write and read data to/from S3" in
    withContainers { s3Container =>
      val configuration = new Configuration()

      configuration.set("fs.s3a.access.key", s3Container.container.getAccessKey())
      configuration.set("fs.s3a.secret.key", s3Container.container.getSecretKey())
      configuration.set("fs.s3a.endpoint", s3Container.container.getEndpoint().toString())
      configuration.set("fs.s3a.endpoint.region", s3Container.container.getRegion())

      ParquetWriter.of[Data].options(ParquetWriter.Options(hadoopConf = configuration)).writeAndClose(path, data)

      Using.resource(ParquetReader.as[Data].options(ParquetReader.Options(hadoopConf = configuration)).read(path)) {
        _.toSeq should be(data)
      }
    }

}
