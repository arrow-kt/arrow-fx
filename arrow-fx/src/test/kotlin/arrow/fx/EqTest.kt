package arrow.fx

import arrow.fx.internal.TimeoutException
import arrow.fx.typeclasses.milliseconds
import arrow.test.UnitSpec
import arrow.test.eq.eq
import arrow.test.laws.equalUnderTheLaw
import arrow.test.laws.shouldBeEq
import arrow.test.laws.shouldNotBeEq
import io.kotlintest.shouldThrow

class EqTest : UnitSpec() {

  init {
    "Should pass pure equal values" {
      IO.just(true).shouldBeEq(IO.just(true), IO.eq())
    }

    "Should fail for pure non-equal values" {
      IO.just(true).shouldNotBeEq(IO.just(false), IO.eq())
    }

    "Times out" {
      shouldThrow<TimeoutException> {
        IO.never.equalUnderTheLaw(IO.just(1), IO.eq(timeout = 10.milliseconds))
      }
    }
  }
}
