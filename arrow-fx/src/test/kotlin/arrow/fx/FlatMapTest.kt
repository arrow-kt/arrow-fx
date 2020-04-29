package arrow.fx

import arrow.core.Either
import arrow.core.Left
import arrow.core.test.UnitSpec
import io.kotlintest.shouldBe
import arrow.fx.extensions.io.monad.flatMap as ioMonadFlatMap
import arrow.fx.flatMap as arrowFxFlatMap

class OneError
class TwoError

class FlatMapTest : UnitSpec() {

  init {

    val one = OneError()

    fun oneIO(): IO<OneError, String> = IO.raiseError(one)
    fun twoIO(value: String): IO<TwoError, Int> = IO.just(value.toInt())

    "ioMonadFlatMap" {
      oneIO()
        .ioMonadFlatMap { twoIO(it) }
        .unsafeRunSyncEither() shouldBe Either.Left(one)
    }

    "ioMonadFlatMap and fold" {
      oneIO()
        .ioMonadFlatMap { twoIO(it) }
        .unsafeRunSyncEither()
        .fold({ it }, { it }) shouldBe one
    }

    "arrowFxFlatMap" {
      oneIO()
        .arrowFxFlatMap { twoIO(it) }
        .unsafeRunSyncEither() shouldBe Left(one)
    }

    "arrowFxFlatMap with fold" {
      oneIO()
        .arrowFxFlatMap { twoIO(it) }
        .unsafeRunSyncEither()
        .fold({ it }, { it }) shouldBe one
    }
  }
}
