package arrow.fx.test.generators

import arrow.Kind
import arrow.core.test.generators.GenK
import arrow.core.test.generators.throwable
import arrow.fx.ForIO
import arrow.fx.IO
import arrow.fx.typeclasses.Fiber
import arrow.fx.typeclasses.FiberPartialOf
import arrow.typeclasses.Applicative
import arrow.typeclasses.ApplicativeError
import io.kotlintest.properties.Gen
import java.util.concurrent.TimeUnit

fun <F, A, E> Gen<E>.raiseError(AP: ApplicativeError<F, E>): Gen<Kind<F, A>> =
  map { AP.raiseError<A>(it) }

fun Gen.Companion.timeUnit(): Gen<TimeUnit> = Gen.from(TimeUnit.values())

fun IO.Companion.genK() = object : GenK<ForIO> {
  override fun <A> genK(gen: Gen<A>): Gen<Kind<ForIO, A>> = Gen.oneOf(
    gen.map(IO.Companion::just),
    Gen.throwable().map(IO.Companion::raiseError)
  )
}

fun <F> Fiber.Companion.genK(A: Applicative<F>) = object : GenK<FiberPartialOf<F>> {
  override fun <A> genK(gen: Gen<A>): Gen<Kind<FiberPartialOf<F>, A>> = gen.map {
    Fiber(A.just(it), A.just(Unit))
  }
}
