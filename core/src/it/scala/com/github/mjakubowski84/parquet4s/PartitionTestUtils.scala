package com.github.mjakubowski84.parquet4s

import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}

trait PartitionTestUtils extends TableDrivenPropertyChecks {
  private val allChars: Seq[Char]          = (Byte.MinValue to Byte.MaxValue).map(_.toChar)
  private val alphaNumericChars: Seq[Char] = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')

  private val allowedPartitionNameChars: Seq[Char]  = alphaNumericChars ++ Seq('.', '_')
  private val allowedPartitionValueChars: Seq[Char] = alphaNumericChars ++ Seq('!', '-', '_', '.', '*', '\'', '(', ')')

  private val disallowedPartitionNameChars: Seq[Char]  = allChars.filterNot(allowedPartitionNameChars.contains)
  private val disallowedPartitionValueChars: Seq[Char] = allChars.filterNot(allowedPartitionValueChars.contains)

  private val validNames  = generatePartitionStrings(prefix = "testValue", withChars = allowedPartitionNameChars)
  private val validValues = generatePartitionStrings(prefix = "testName", withChars = allowedPartitionValueChars)

  private val invalidNames  = generatePartitionStrings(prefix = "testValue", withChars = disallowedPartitionNameChars)
  private val invalidValues = generatePartitionStrings(prefix = "testName", withChars = disallowedPartitionValueChars)

  private def generatePartitionStrings(prefix: String, withChars: Seq[Char]) = withChars.map(char => s"$prefix$char")

  val ValidPartitionsTable: TableFor2[String, String] = Table(
    ("name", "value"),
    validNames.flatMap(name => validValues.map(value => name -> value))*
  )
  val InvalidPartitionsTable: TableFor2[String, String] = Table(
    ("name", "value"),
    invalidNames.flatMap(name => invalidValues.map(value => name -> value))*
  )

}
