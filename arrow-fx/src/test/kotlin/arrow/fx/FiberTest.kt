package arrow.fx

import arrow.Kind
import arrow.core.extensions.monoid
import arrow.fx.extensions.applicative
import arrow.fx.extensions.functor
import arrow.fx.extensions.io.applicative.applicative
import arrow.fx.extensions.io.concurrent.concurrent
import arrow.fx.extensions.monoid
import arrow.fx.typeclasses.Fiber
import arrow.fx.typeclasses.FiberPartialOf
import arrow.fx.typeclasses.fix
import arrow.test.UnitSpec
import arrow.test.eq.EQ
import arrow.test.eq.eq
import arrow.test.generators.GenK
import arrow.test.laws.ApplicativeLaws
import arrow.test.laws.MonoidLaws
import arrow.typeclasses.Applicative
import arrow.typeclasses.Eq
import arrow.typeclasses.EqK
import io.kotlintest.properties.Gen

class FiberTest : UnitSpec() {

  init {

    fun EQK() = object : EqK<FiberPartialOf<ForIO>> {
      override fun <A> Kind<FiberPartialOf<ForIO>, A>.eqK(other: Kind<FiberPartialOf<ForIO>, A>, EQ: Eq<A>): Boolean =
        IO.eq<A>().run {
          this@eqK.fix().join().eqv(other.fix().join())
        }
    }

    fun <F> GENK(A: Applicative<F>) = object : GenK<FiberPartialOf<F>> {
      override fun <A> genK(gen: Gen<A>): Gen<Kind<FiberPartialOf<F>, A>> = gen.map {
        Fiber(A.just(it), A.just(Unit))
      }
    }

    testLaws(
      ApplicativeLaws.laws<FiberPartialOf<ForIO>>(Fiber.applicative(IO.concurrent()), Fiber.functor(IO.concurrent()), GENK(IO.applicative()), EQK()),
      MonoidLaws.laws(Fiber.monoid(IO.concurrent(), Int.monoid()), Gen.int().map { i ->
        Fiber(IO.just(i), IO.unit)
      }, EQ(IO.eq()))
    )
  }
}
