package com.github.mjakubowski84.parquet4s

import org.apache.parquet.io.api.Binary
import scalapb.GeneratedMessage
import scalapb.descriptors.*
import com.github.mjakubowski84.parquet4s.ScalaPBImplicits.*

import scala.collection.mutable
import scala.util.control.NoStackTrace

case class ScalaPBParquetEncodeException(msg: String) extends RuntimeException(msg) with NoStackTrace

class ScalaPBParquetRecordEncoder[T <: GeneratedMessage] extends ParquetRecordEncoder[T] {
  private val enumMetadata: mutable.Map[String, mutable.Map[String, Int]] = mutable.Map.empty

  override def encode(
      entity: T,
      resolver: EmptyRowParquetRecordResolver,
      configuration: ValueCodecConfiguration
  ): RowParquetRecord =
    encodeMessage(entity.toPMessage)

  override def getExtraMetadata(): Map[String, String] =
    enumMetadata.iterator.map { case (key, value) =>
      val metadataEnumKey    = MetadataEnumPrefix + key
      val metadataEnumValues = value.map { case (name, number) => s"$name:$number" }.mkString(",")
      (metadataEnumKey, metadataEnumValues)
    }.toMap

  private def encodeMessage(msg: PMessage): RowParquetRecord =
    RowParquetRecord(msg.value.view.map { case (fd, v) => fd.name -> encodeField(fd, v) }.toSeq)

  private def encodeField(fd: FieldDescriptor, value: PValue): Value =
    (fd.scalaType, value) match {
      case (ScalaType.Boolean, PBoolean(value)) => BooleanValue(value)
      case (ScalaType.Int, PInt(value))         => IntValue(value)
      case (ScalaType.Long, PLong(value))       => LongValue(value)
      case (ScalaType.Float, PFloat(value))     => FloatValue(value)
      case (ScalaType.Double, PDouble(value))   => DoubleValue(value)
      case (ScalaType.String, PString(value))   => BinaryValue(Binary.fromString(value))
      case (ScalaType.ByteString, PByteString(value)) =>
        BinaryValue(Binary.fromReusedByteBuffer(value.asReadOnlyByteBuffer()))
      case (ScalaType.Message(_), msg: PMessage)                                              => encodeMessage(msg)
      case (ScalaType.Message(md), PRepeated(values)) if fd.isMapField && md.fields.size == 2 => encodeMap(md, values)
      case (_, PRepeated(values))            => ListParquetRecord(values.map(encodeField(fd, _)): _*)
      case (_, PEmpty)                       => NullValue
      case (ScalaType.Enum(_), PEnum(value)) => encodeEnumValue(value)
      case _ =>
        throw ScalaPBParquetEncodeException(s"Unsupported combination of field and value: ${fd.scalaType}, $value")
    }

  private def encodeMap(md: Descriptor, values: Vector[PValue]) = {
    val entries = values.map {
      case msg: PMessage if msg.value.size == 2 => encodeMapEntry(md.fields(0), md.fields(1), msg)
      case value                                => throw ScalaPBParquetEncodeException(s"Invalid map entry: $value")
    }
    MapParquetRecord(entries: _*)
  }

  private def encodeMapEntry(keyFd: FieldDescriptor, valueFd: FieldDescriptor, msg: PMessage) =
    (msg.value.get(keyFd), msg.value.get(valueFd)) match {
      case (Some(key), Some(value)) => (encodeField(keyFd, key), encodeField(valueFd, value))
      case _ =>
        throw ScalaPBParquetEncodeException(s"Invaid map entry: key field: $keyFd, value field: $valueFd, msg: $msg")
    }

  private def encodeEnumValue(value: EnumValueDescriptor) = {
    val enumName = value.containingEnum.fullName
    enumMetadata.getOrElseUpdate(enumName, mutable.Map.empty).put(value.name, value.number)
    BinaryValue(Binary.fromString(value.name))
  }
}
