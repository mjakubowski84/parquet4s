package com.github.mjakubowski84.parquet4s
import java.util.UUID

import akka.stream.FlowShape
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Sink, ZipWith}
import org.apache.hadoop.fs.Path
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.FiniteDuration

object IndefiniteStreamParquetSink extends IOOps {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  type ChunkPathBuilder[In] = (Path, Seq[In]) => Path

  private def defaultChunkPathBuilder[In]: ChunkPathBuilder[In] =
    (basePath, _) => basePath.suffix(s"/part-${UUID.randomUUID()}.parquet")

  def apply[In, ToWrite: ParquetWriter, Mat](path: Path,
                              maxChunkSize: Int,
                              chunkWriteTimeWindow: FiniteDuration,
                              buildChunkPath: ChunkPathBuilder[In] = defaultChunkPathBuilder,
                              preWriteTransformation: In => ToWrite = identity _,
                              postWriteSink: Sink[Seq[In], Mat] = Sink.ignore,
                              options: ParquetWriter.Options = ParquetWriter.Options()
                            ): Sink[In, Mat] = {
    validateWritePath(path, options)

    val internalFlow = Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
    
      val inChunkFlow = b.add(Flow[In].groupedWithin(maxChunkSize, chunkWriteTimeWindow))
      val broadcastChunks = b.add(Broadcast[Seq[In]](outputPorts = 2))
      val writeFlow = Flow[Seq[In]].map { chunk =>
        val toWrite = chunk.map(preWriteTransformation)
        val chunkPath = buildChunkPath(path, chunk)
        if (logger.isDebugEnabled()) logger.debug(s"Writing ${toWrite.size} records to $chunkPath")
        ParquetWriter.write(chunkPath.toString, toWrite)
      }
      val zip = b.add(ZipWith[Seq[In], Unit, Seq[In]]((chunk, _) => chunk))
      
      inChunkFlow ~> broadcastChunks ~> writeFlow ~> zip.in1
                     broadcastChunks ~> zip.in0

      FlowShape(inChunkFlow.in, zip.out)               
    })

    internalFlow.toMat(postWriteSink)(Keep.right)
  }

}
