---
layout: docs
title: Integration with FS2
permalink: docs/fs2/
---

# Integration with FS2

FS2 integration allows you to read and write Parquet using functional streams. Functionality is exactly the same as in the case of Akka module. In order to use it, please import the following:

```scala
"com.github.mjakubowski84" %% "parquet4s-fs2" % "@VERSION@"
"org.apache.hadoop" % "hadoop-client" % yourHadoopVersion
```

`parquet` object has a single `Stream` for reading a single file or a directory (can be [partitioned]({% link docs/partitioning.md %})), a `Pipe` for writing a single file and a sophisticated `Pipe` for performing complex writes.

```scala mdoc:compile-only
import cats.effect.{IO, IOApp}
import com.github.mjakubowski84.parquet4s.parquet.{fromParquet, writeSingleFile, viaParquet}
import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter, Path}
import fs2.Stream
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetWriter => HadoopParquetWriter}
import org.apache.hadoop.conf.Configuration

import scala.concurrent.duration._

object Example extends IOApp.Simple {

  case class User(userId: String, name: String, created: java.sql.Timestamp)

  val users: Stream[IO, User] = ???

  val conf: Configuration = ??? // Set Hadoop configuration programmatically

  // Please check all the available configuration options!
  val writeOptions = ParquetWriter.Options(
    writeMode = Mode.OVERWRITE,
    compressionCodecName = CompressionCodecName.SNAPPY,
    hadoopConf = conf // optional hadoopConf
  )

  // Writes a single file.
  val writeSingleFilePipe = 
    writeSingleFile[IO]
      .of[User]
      .options(writeOptions)
      .write(Path("file:///data/users/single.parquet"))

  // (Experimental API) Writes a single file using a custom ParquetWriter.
  class UserParquetWriterBuilder(path: Path) extends HadoopParquetWriter.Builder[User, UserParquetWriterBuilder](path.toHadoop) {
    override def self() = this
    override def getWriteSupport(conf: Configuration) = ???
  }
  val writeSingleFileCustomPipe =
    writeSingleFile[IO]
      .custom[User, UserParquetWriterBuilder](new UserParquetWriterBuilder(Path("file:///data/users/custom.parquet")))
      .options(writeOptions)
      .write

  // Tailored for writing indefinite streams.
  // Writes file when chunk reaches size limit and when defined time period elapses.
  // Can also partition files!
  // Check all the parameters and example usage in project sources.
  val writeRotatedPipe =
    viaParquet[IO]
      .of[User]
      .maxCount(writeOptions.rowGroupSize)
      .maxDuration(30.seconds)
      .options(writeOptions)
      .write(Path("file:///data/users"))

  // Reads a file, files from the directory or a partitioned directory. 
  // Please also have a look at the rest of parameters.
  val readAllStream =
    fromParquet[IO]
      .as[User]
      .options(ParquetReader.Options(hadoopConf = conf))
      .read(Path("file:///data/users"))
      .printlns

  def run: IO[Unit] =  
    users
      .through(writeRotatedPipe)
      .through(writeSingleFilePipe)
      .through(writeSingleFileCustomPipe)
      .append(readAllStream)
      .compile
      .drain
  
}
```


---
What differentiates FS2 from Akka is that, for better performance, FS2 processes stream elements in chunks. Therefore, `viaParquet` and `fromParquet` have a `chunkSize` property that allows a custom definition of the chunk size. The default value is `16`. Override the value to set up your own balance between memory consumption and performance.

---


Please check the [examples](https://github.com/mjakubowski84/parquet4s/tree/master/examples/src/main/scala/com/github/mjakubowski84/parquet4s/fs2) to learn more.
