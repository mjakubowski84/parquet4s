package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.parquet.io.{InputFile, OutputFile, PositionOutputStream}

import java.io.ByteArrayOutputStream

object InMemoryOutputFile {
  val DefaultBlockSize: Int = 64 << 10

  def apply(
      initBufferSize: Int,
      maxBufferSize: Option[Int] = None,
      blockSize: Int             = DefaultBlockSize
  ): InMemoryOutputFile = new InMemoryOutputFile(initBufferSize, maxBufferSize.getOrElse(3 * initBufferSize), blockSize)
}

/** Reusable in-memory `OutputFile` based on `ByteArrayOutputStream`
  *
  * @param initBufferSize
  *   size of the `ByteArrayOutputStream`'s internal buffer when it is created
  * @param maxBufferSize
  *   a threshold beyond which the internal buffer will be recreated with the initBufferSize
  * @param blockSize
  *   size of a row group being buffered in memory. This limits the memory usage when writing
  */
class InMemoryOutputFile private (initBufferSize: Int, maxBufferSize: Int, blockSize: Int) extends OutputFile {
  private val os = new ReusableByteArrayOutputStream(initBufferSize, maxBufferSize)

  override def create(blockSizeHint: Long): PositionOutputStream = {
    if (os.size() > 0) throw new FileAlreadyExistsException(s"In-memory file already exists")
    new PositionOutputStream {
      override def getPos: Long                                    = os.size().toLong
      override def write(b: Int): Unit                             = os.write(b)
      override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)
    }
  }

  override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream = {
    os.reset()
    create(blockSizeHint)
  }

  override def supportsBlockSize(): Boolean = true

  override def defaultBlockSize(): Long = blockSize.toLong

  /** Return an Array[Byte] copied from the current content of the internal buffer, and reset the internal state. The
    * [[InMemoryOutputFile]] could then be reused without allocating the internal buffer.
    *
    * @return
    *   bytes copied from the current content of internal buffer
    */
  def take(): Array[Byte] = os.take

  def contentLength: Int = os.size()

  /** Creates an [[org.apache.parquet.io.InputFile]] from the content of this [[org.apache.parquet.io.OutputFile]].
    */
  def toInputFile: InputFile = InMemoryInputFile.fromBytes(take())
}

class ReusableByteArrayOutputStream(initBufferSize: Int, maxBufferSize: Int)
    extends ByteArrayOutputStream(initBufferSize) {
  def take: Array[Byte] = {
    val content = toByteArray
    if (buf.length > maxBufferSize) {
      buf = new Array[Byte](initBufferSize)
    }
    count = 0
    content
  }
}
