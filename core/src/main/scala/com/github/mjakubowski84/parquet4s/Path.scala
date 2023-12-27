package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path as HadoopPath
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}
import org.apache.parquet.io.{InputFile, OutputFile}

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
class Path private (val hadoopPath: HadoopPath) extends AnyVal {

  def append(element: String): Path = new Path(new HadoopPath(hadoopPath, element))

  def parent: Option[Path] = Option(hadoopPath.getParent).map(Path.apply)

  def name: String = hadoopPath.getName

  def toUri: URI = hadoopPath.toUri

  def toNio: NioPath = Paths.get(toUri)

  def toHadoop: HadoopPath = hadoopPath

  def canEqual(other: Any): Boolean = other.isInstanceOf[Path]

  def toOutputFile(conf: Configuration): OutputFile = HadoopOutputFile.fromPath(hadoopPath, conf)

  def toOutputFile(options: ParquetWriter.Options): OutputFile = toOutputFile(options.hadoopConf)

  def toInputFile(conf: Configuration): InputFile = HadoopInputFile.fromPath(hadoopPath, conf)

  def toInputFile(options: ParquetReader.Options): InputFile = HadoopInputFile.fromPath(hadoopPath, options.hadoopConf)

  override def toString: String = hadoopPath.toString

}
