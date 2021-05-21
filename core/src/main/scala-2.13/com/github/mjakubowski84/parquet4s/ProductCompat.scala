package com.github.mjakubowski84.parquet4s

trait ProductCompat {

  this: RowParquetRecord =>

  override def productArity: Int = fields.size

  override def productElement(n: Int): Any = get(fields.get(n))

  override def productElementNames: Iterator[String] = fields.iterator

  override def productElementName(n: Int): String = fields.get(n)

  // TODO product prefix??? should we store the name?
  
}