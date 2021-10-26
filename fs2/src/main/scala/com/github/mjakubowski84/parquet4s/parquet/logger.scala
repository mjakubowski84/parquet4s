package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.Sync
import cats.implicits.*
import org.slf4j.LoggerFactory

import scala.language.higherKinds

private[parquet] object logger {

  class Logger[F[_]](wrapped: org.slf4j.Logger)(implicit F: Sync[F]) {

    def debug(msg: => String): F[Unit] =
      F.catchNonFatal(wrapped.isDebugEnabled).flatMap {
        case true =>
          F.delay(wrapped.debug(msg))
        case false =>
          F.unit
      }

  }

  def apply[F[_]](name: String)(implicit F: Sync[F]): F[Logger[F]] =
    F.delay(LoggerFactory.getLogger(name)).map(new Logger(_))

  def apply[F[_]: Sync](clazz: Class[?]): F[Logger[F]] =
    apply(clazz.getCanonicalName)

}
