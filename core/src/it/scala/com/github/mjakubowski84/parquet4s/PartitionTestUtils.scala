package com.github.mjakubowski84.parquet4s

trait PartitionTestUtils {
  val allChars: Seq[Char] = (Byte.MinValue to Byte.MaxValue).map(_.toChar)
  val alphaNumericChars: Seq[Char] = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')

  val allowedPartitionNameChars: Seq[Char] = alphaNumericChars ++ Seq('.', '_')
  val allowedPartitionValueChars: Seq[Char] = alphaNumericChars ++ Seq('!', '-', '_', '.', '*', '\'', '(', ')')

  val disallowedPartitionNameChars: Seq[Char] = allChars.filterNot(allowedPartitionNameChars.contains)
  val disallowedPartitionValueChars: Seq[Char] = allChars.filterNot(allowedPartitionValueChars.contains)

  def generatePartitionStrings(prefix: String, withChars: Seq[Char]): Seq[String] =
    withChars.map(char => s"$prefix$char")
}
