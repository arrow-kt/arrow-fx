package arrow.test.eq

import arrow.Kind
import arrow.core.extensions.either.eq.eq
import arrow.fx.ForIO
import arrow.fx.IO
import arrow.fx.extensions.io.applicative.applicative
import arrow.fx.extensions.io.applicativeError.attempt
import arrow.fx.extensions.io.concurrent.waitFor
import arrow.fx.fix
import arrow.fx.typeclasses.Duration
import arrow.fx.typeclasses.FiberOf
import arrow.fx.typeclasses.fix
import arrow.fx.typeclasses.seconds
import arrow.typeclasses.Eq
import arrow.typeclasses.EqK

fun <A> IO.Companion.eq(EQA: Eq<A> = Eq.any(), timeout: Duration = 5.seconds): Eq<Kind<ForIO, A>> = Eq { a, b ->
  arrow.core.Either.eq(Eq.any(), EQA).run {
    IO.applicative().mapN(a.attempt(), b.attempt()) { (a, b) -> a.eqv(b) }
      .waitFor(timeout)
      .unsafeRunSync()
  }
}

fun IO.Companion.eqK() = object : EqK<ForIO> {
  override fun <A> Kind<ForIO, A>.eqK(other: Kind<ForIO, A>, EQ: Eq<A>): Boolean = eq(EQ).run {
    fix().eqv(other.fix())
  }
}

fun <F, A> EQ(EQ: Eq<Kind<F, A>>): Eq<FiberOf<F, A>> = object : Eq<FiberOf<F, A>> {
  override fun FiberOf<F, A>.eqv(b: FiberOf<F, A>): Boolean = EQ.run {
    fix().join().eqv(b.fix().join())
  }
}
