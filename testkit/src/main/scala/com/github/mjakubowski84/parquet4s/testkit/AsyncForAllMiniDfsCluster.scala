package com.github.mjakubowski84.parquet4s.testkit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.scalatest.{Args, AsyncTestSuite, AsyncTestSuiteMixin, CompositeStatus, Status, Suite}

import java.nio.file.Files
import scala.concurrent.Future

trait AsyncForAllMiniDfsCluster extends AsyncTestSuiteMixin {
  self: AsyncTestSuite =>

  private lazy val cluster = {
    val baseDir       = Files.createTempDirectory("").toFile
    val configuration = new Configuration()
    new MiniDFSCluster.Builder(configuration, baseDir).format(true).build
  }

  protected val deleteDfsDirOnShutdown: Boolean

  def hadoopConfiguration: Configuration =
    cluster.getConfiguration(0)

  abstract override def withFixture(test: NoArgAsyncTest) =
    complete {
      cluster // run cluster
      super.withFixture(test)
    } lastly {
      cluster.shutdown(deleteDfsDirOnShutdown)
    }
}
