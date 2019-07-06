package com.github.mjakubowski84

import java.util.UUID

import org.apache.hadoop.fs.Path

package object parquet4s {

  object ChunkPathBuilder {

    def default[In]: ChunkPathBuilder[In] =
      (basePath: Path, _) => basePath.suffix(s"/part-${UUID.randomUUID()}.parquet")

  }

  type ChunkPathBuilder[-In] = (Path, Seq[In]) => Path

}
