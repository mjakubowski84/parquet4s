package com.github.mjakubowski84.parquet4s

import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IOOpsSpec extends AnyFlatSpec with Matchers with Inside with PartitionTestUtils {

  "PartitionRegexp" should "match valid partition names and values" in {
    forAll(ValidPartitionsTable) { case (name, value) =>
      inside(s"$name=$value") { case IOOps.PartitionRegexp(`name`, `value`) =>
        succeed
      }
    }
  }

  it should "not match invalid partition names and values" in {
    forAll(InvalidPartitionsTable) { case (name, value) =>
      s"$name=$value" match {
        case IOOps.PartitionRegexp(capturedName, capturedValue) =>
          fail(
            s"Expected no match for name [$name] and value [$value] " +
              s"but one was found: [$capturedName, $capturedValue]"
          )
        case _ =>
          succeed
      }
    }
  }

}
