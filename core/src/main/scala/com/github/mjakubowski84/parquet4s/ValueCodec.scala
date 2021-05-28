package com.github.mjakubowski84.parquet4s

/**
 * Represents both [[ValueEncoder]] and [[ValueDecoder]]
 */
trait ValueCodec[T] extends ValueEncoder[T] with ValueDecoder[T]

/**
 * Represents both [[RequiredValueEncoder]] and [[RequiredValueDecoder]]
 */
trait RequiredValueCodec[T] extends ValueCodec[T] with RequiredValueEncoder[T] with RequiredValueDecoder[T]

/**
 * Represents both [[OptionalValueEncoder]] and [[OptionalValueDecoder]]
 */
trait OptionalValueCodec[T] extends ValueCodec[T] with OptionalValueEncoder[T] with OptionalValueDecoder[T]
