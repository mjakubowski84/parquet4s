package com.github.mjakubowski84.parquet4s.testkit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.scalatest.{Args, CompositeStatus, Status, Suite, SuiteMixin}

import java.nio.file.Files

trait ForAllMiniDfsCluster extends SuiteMixin {
  self: Suite =>

  private lazy val cluster = {
    val baseDir       = Files.createTempDirectory("").toFile
    val configuration = new Configuration()
    new MiniDFSCluster.Builder(configuration, baseDir).format(true).build
  }

  protected val deleteDfsDirOnShutdown: Boolean

  def hadoopConfiguration: Configuration =
    cluster.getConfiguration(0)

  abstract override def run(testName: Option[String], args: Args): Status =
    if (expectedTestCount(args.filter) == 0) {
      new CompositeStatus(Set.empty)
    } else {
      try {
        cluster // run cluster
        super.run(testName, args)
      } finally cluster.shutdown(deleteDfsDirOnShutdown)
    }
}
