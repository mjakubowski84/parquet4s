package com.github.mjakubowski84.parquet4s.akkaPekko.indefinite

trait Logger {
  lazy val logger: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}
