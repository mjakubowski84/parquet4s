package com.github.mjakubowski84.parquet4s

private[parquet4s] object Join {

  def left(
      left: ParquetIterable[RowParquetRecord],
      right: ParquetIterable[RowParquetRecord],
      onLeft: ColumnPath,
      onRight: ColumnPath
  ): ParquetIterable[RowParquetRecord] = (new JoinImpl(right, onLeft, onRight) with LeftJoin).apply(left)

  def right(
      left: ParquetIterable[RowParquetRecord],
      right: ParquetIterable[RowParquetRecord],
      onLeft: ColumnPath,
      onRight: ColumnPath
  ): ParquetIterable[RowParquetRecord] = (new JoinImpl(right, onLeft, onRight) with RightJoin).apply(left)

  def inner(
      left: ParquetIterable[RowParquetRecord],
      right: ParquetIterable[RowParquetRecord],
      onLeft: ColumnPath,
      onRight: ColumnPath
  ): ParquetIterable[RowParquetRecord] = new JoinImpl(right, onLeft, onRight).apply(left)

  def full(
      left: ParquetIterable[RowParquetRecord],
      right: ParquetIterable[RowParquetRecord],
      onLeft: ColumnPath,
      onRight: ColumnPath
  ): ParquetIterable[RowParquetRecord] = (new JoinImpl(right, onLeft, onRight) with LeftJoin with RightJoin).apply(left)

}

private class JoinImpl(
    override val right: ParquetIterable[RowParquetRecord],
    override val onLeft: ColumnPath,
    override val onRight: ColumnPath
) extends Join

private trait Join {
  protected val right: ParquetIterable[RowParquetRecord]
  protected val onLeft: ColumnPath
  protected val onRight: ColumnPath

  protected lazy val rightMapping: Map[Option[Value], Iterable[RowParquetRecord]] = right.groupBy(_.get(onRight))
  protected lazy val emptyRightRecord: RowParquetRecord = rightMapping.headOption
    .map(_._2.head.iterator.map(_._1).toList)
    .fold(RowParquetRecord.EmptyNoSchema)(RowParquetRecord.emptyWithSchema)
  private var emptyLeftRecordOpt: Option[RowParquetRecord] = None
  protected lazy val emptyLeftRecord: RowParquetRecord =
    emptyLeftRecordOpt.getOrElse(RowParquetRecord.EmptyNoSchema)

  def matchFound(
      leftRecord: RowParquetRecord,
      key: Option[Value],
      matchingRightRecords: Iterable[RowParquetRecord]
  ): Iterable[RowParquetRecord] = matchingRightRecords.map(leftRecord.merge)

  def matchNotFound(leftRecord: RowParquetRecord): Iterable[RowParquetRecord] = Iterable.empty

  def apply(left: ParquetIterable[RowParquetRecord]): ParquetIterable[RowParquetRecord] =
    left
      .appendTransformation { leftRecord =>
        if (emptyLeftRecordOpt.isEmpty) {
          emptyLeftRecordOpt = Option(RowParquetRecord.emptyWithSchema(leftRecord.iterator.map(_._1).toList))
        }
        val key = leftRecord.get(onLeft)
        rightMapping.get(key) match {
          case Some(matchingRightRecords) =>
            matchFound(leftRecord, key, matchingRightRecords)
          case None =>
            matchNotFound(leftRecord)
        }
      }
}

private trait LeftJoin extends Join {
  override def matchNotFound(leftRecord: RowParquetRecord): Iterable[RowParquetRecord] =
    Iterable(leftRecord.merge(emptyRightRecord))
}

private trait RightJoin extends Join {
  private var matchedKeys: Set[Option[Value]] = Set.empty[Option[Value]]

  override def matchFound(
      leftRecord: RowParquetRecord,
      key: Option[Value],
      matchingRightRecords: Iterable[RowParquetRecord]
  ): Iterable[RowParquetRecord] = {
    matchedKeys = matchedKeys + key
    matchingRightRecords.map(leftRecord.merge)
  }

  override def apply(left: ParquetIterable[RowParquetRecord]): ParquetIterable[RowParquetRecord] =
    super[Join].apply(left).concat {
      new InMemoryParquetIterable[RowParquetRecord](
        data = rightMapping.flatMap {
          case (key, _) if matchedKeys.contains(key) => Iterable.empty
          case (_, records)                          => records
        },
        valueCodecConfiguration = right.valueCodecConfiguration,
        transformations         = Seq(record => Option(emptyLeftRecord.merge(record)))
      )
    }
}
