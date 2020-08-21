package arrow.fx.data

import arrow.core.test.UnitSpec
import io.kotlintest.properties.Gen
import io.kotlintest.properties.forAll

class DurationTest : UnitSpec() {

  init {
    "plus should be commutative" {
      forAll(Gen.duration(), Gen.duration()) { a, b ->
        a + b == b + a
      }
    }

    "comparison should correct in both directions" {
      forAll(Gen.duration(), Gen.duration()) { a, b ->
        a < b == b > a
      }
    }
  }
}
