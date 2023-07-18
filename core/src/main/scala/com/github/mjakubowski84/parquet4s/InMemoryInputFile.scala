package com.github.mjakubowski84.parquet4s

import org.apache.parquet.io.{InputFile, SeekableInputStream}

import java.io.EOFException
import java.nio.ByteBuffer
import scala.util.control.NoStackTrace

object InMemoryInputFile {
  def fromBytesUnsafe(bytes: Array[Byte]): InMemoryInputFile = new InMemoryInputFile(bytes)

  def fromBytes(bytes: Array[Byte]): InMemoryInputFile = new InMemoryInputFile(bytes.clone())
}

class InMemoryInputFile private (content: Array[Byte]) extends InputFile {

  override def getLength: Long = content.length

  override def newStream(): SeekableInputStream = new SeekableInputStream {
    private var pos: Int = 0

    override def getPos: Long = pos

    override def seek(newPos: Long): Unit = pos = newPos.toInt

    override def readFully(bytes: Array[Byte]): Unit = readFully(bytes, 0, bytes.length)

    override def readFully(bytes: Array[Byte], start: Int, len: Int): Unit = {
      if (content.length - pos < len) throw new EOFException with NoStackTrace
      System.arraycopy(content, pos, bytes, start, len)
      pos += len
    }

    override def read(buf: ByteBuffer): Int = {
      val avail = remaining
      if (avail == 0) -1
      else {
        val len = avail.min(buf.remaining())
        if (len > 0) {
          buf.put(content, pos, len)
          pos += len
        }
        len
      }
    }

    override def readFully(buf: ByteBuffer): Unit = {
      val availSpace = buf.remaining
      if (remaining < availSpace) throw new EOFException with NoStackTrace
      if (availSpace > 0) buf.put(content, pos, availSpace)
      pos += availSpace
    }

    override def read(): Int =
      if (remaining == 0) -1
      else {
        val next = content(pos).toInt
        pos += 1
        next
      }

    private def remaining: Int = content.length - pos
  }
}
