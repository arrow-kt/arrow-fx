package arrow.fx.test.generators

import arrow.Kind
import arrow.core.Eval
import arrow.core.test.generators.GenK
import arrow.core.test.generators.intSmall
import arrow.core.test.generators.throwable
import arrow.fx.DecisionPartialOf
import arrow.fx.ForIO
import arrow.fx.IO
import arrow.fx.Schedule
import arrow.fx.typeclasses.Fiber
import arrow.fx.typeclasses.FiberPartialOf
import arrow.fx.typeclasses.nanoseconds
import arrow.typeclasses.Applicative
import arrow.typeclasses.ApplicativeError
import io.kotest.property.Arb
import io.kotest.property.arbitrary.bind
import io.kotest.property.arbitrary.bool
import io.kotest.property.arbitrary.choice
import io.kotest.property.arbitrary.map
import io.kotest.property.arbitrary.of
import java.util.concurrent.TimeUnit

fun <F, A, E> Arb<E>.raiseError(AP: ApplicativeError<F, E>): Arb<Kind<F, A>> =
  map { AP.raiseError<A>(it) }

fun Arb.Companion.timeUnit(): Arb<TimeUnit> = Arb.of(*TimeUnit.values())

fun IO.Companion.genK() = object : GenK<ForIO> {
  override fun <A> genK(gen: Arb<A>): Arb<Kind<ForIO, A>> = Arb.choice(
    gen.map(IO.Companion::just),
    Arb.throwable().map(IO.Companion::raiseError)
  )
}

fun <F> Fiber.Companion.genK(A: Applicative<F>) = object : GenK<FiberPartialOf<F>> {
  override fun <A> genK(gen: Arb<A>): Arb<Kind<FiberPartialOf<F>, A>> = gen.map {
    Fiber(A.just(it), A.just(Unit))
  }
}

fun Schedule.Decision.Companion.genK(): GenK<DecisionPartialOf<Any?>> = object : GenK<DecisionPartialOf<Any?>> {
  override fun <A> genK(gen: Arb<A>): Arb<Kind<DecisionPartialOf<Any?>, A>> =
    Arb.bind(
      Arb.bool(),
      Arb.intSmall(),
      gen
    ) { cont, delay, res -> Schedule.Decision(cont, delay.nanoseconds, 0 as Any?, Eval.now(res)) }
}
