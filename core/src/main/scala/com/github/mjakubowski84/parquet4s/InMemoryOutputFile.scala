package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.parquet.io.{OutputFile, PositionOutputStream}

import java.io.ByteArrayOutputStream
import scala.language.reflectiveCalls
import scala.util.control.NoStackTrace

object InMemoryOutputFile {
  def apply(name: String): InMemoryOutputFile = new InMemoryOutputFile(name)
}

class InMemoryOutputFile private (name: String) extends OutputFile {
  private val os = new ByteArrayOutputStream()

  override def create(blockSizeHint: Long): PositionOutputStream = {
    if (os.size() > 0) throw new FileAlreadyExistsException(s"In-memory file already exists: $name")
    new PositionOutputStream {
      override def getPos: Long                                    = os.size()
      override def write(b: Int): Unit                             = os.write(b)
      override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)
    }
  }

  override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream = {
    os.reset()
    create(blockSizeHint)
  }

  override def supportsBlockSize(): Boolean = false

  override def defaultBlockSize(): Long =
    throw new UnsupportedOperationException("Block size is not supported by InMemoryOutputFile") with NoStackTrace

  def toByteArray: Array[Byte] = os.toByteArray
}
