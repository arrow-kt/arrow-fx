package arrow.fx

import arrow.fx.internal.TimeoutException
import arrow.fx.typeclasses.milliseconds
import arrow.test.UnitSpec
import arrow.test.eq.eq
import arrow.test.laws.equalUnderTheLaw
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow

class EqTest : UnitSpec() {

  init {
    "Should pass pure equal values" {
      IO.just(true).equalUnderTheLaw(IO.just(true), IO.eq()) shouldBe true
    }

    "Should fail for pure non-equal values" {
      IO.just(true).equalUnderTheLaw(IO.just(false), IO.eq()) shouldBe false
    }

    "Times out" {
      shouldThrow<TimeoutException> {
        IO.never.equalUnderTheLaw(IO.just(1), IO.eq(timeout = 10.milliseconds))
      }
    }
  }
}
