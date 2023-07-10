package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ParquetWriter.Options
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path as HadoopPath
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.parquet.io.OutputFile

import java.net.URI
import java.nio.file.{Paths, Path as NioPath}

object Path {

  def apply(hadoopPath: HadoopPath): Path = new Path(hadoopPath)

  def apply(pathString: String): Path = apply(new HadoopPath(pathString))

  def apply(nioPath: NioPath): Path = apply(new URI("file", null, nioPath.toAbsolutePath.toString, null, null))

  def apply(uri: URI): Path = apply(new HadoopPath(uri))

  def apply(parent: Path, child: String): Path = apply(new HadoopPath(parent.toHadoop, child))

}

/** Represents path/URI to Parquet file or directory containing Parquet files.
  */
class Path private (val hadoopPath: HadoopPath) {

  def append(element: String): Path = new Path(new HadoopPath(hadoopPath, element))

  def parent: Option[Path] = Option(hadoopPath.getParent).map(Path.apply)

  def name: String = hadoopPath.getName

  def toUri: URI = hadoopPath.toUri

  def toNio: NioPath = Paths.get(toUri)

  def toHadoop: HadoopPath = hadoopPath

  def canEqual(other: Any): Boolean = other.isInstanceOf[Path]

  def toOutputFile(conf: Configuration): OutputFile = HadoopOutputFile.fromPath(hadoopPath, conf)

  def toOutputFile(options: Options): OutputFile = toOutputFile(options.hadoopConf)

  override def equals(other: Any): Boolean = other match {
    case that: Path =>
      (that canEqual this) &&
        hadoopPath == that.toHadoop
    case _ => false
  }

  override def hashCode(): Int = hadoopPath.hashCode()

  override def toString: String = hadoopPath.toString

}
