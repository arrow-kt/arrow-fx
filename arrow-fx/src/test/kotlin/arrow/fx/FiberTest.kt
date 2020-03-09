package arrow.fx

import arrow.core.extensions.monoid
import arrow.fx.extensions.applicative
import arrow.fx.extensions.functor
import arrow.fx.extensions.io.applicative.applicative
import arrow.fx.extensions.io.concurrent.concurrent
import arrow.fx.extensions.monoid
import arrow.fx.typeclasses.Fiber
import arrow.test.UnitSpec
import arrow.test.eq.eq
import arrow.test.eq.eqK
import arrow.test.generators.genK
import arrow.test.laws.ApplicativeLaws
import arrow.test.laws.MonoidLaws
import io.kotlintest.properties.Gen

class FiberTest : UnitSpec() {

  init {
    testLaws(
      ApplicativeLaws.laws(Fiber.applicative(IO.concurrent()), Fiber.functor(IO.concurrent()), Fiber.genK(IO.applicative()), Fiber.eqK()),
      MonoidLaws.laws(Fiber.monoid(IO.concurrent(), Int.monoid()), Gen.int().map { i ->
        Fiber(IO.just(i), IO.unit)
      }, Fiber.eq(IO.eq()))
    )
  }
}
