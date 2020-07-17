package arrow.fx.test.laws

import arrow.core.extensions.either.eq.eq
import arrow.fx.IO
import arrow.fx.IOOf
import arrow.fx.fix
import arrow.fx.extensions.io.applicative.applicative
import arrow.fx.extensions.io.concurrent.waitFor
import arrow.fx.typeclasses.Duration
import arrow.fx.typeclasses.seconds
import arrow.typeclasses.Eq
import io.kotest.matchers.Matcher
import io.kotest.matchers.MatcherResult
import io.kotest.matchers.should
import io.kotest.matchers.shouldNot

fun <A> A.equalUnderTheLaw(b: A, eq: Eq<A>): Boolean =
  shouldBeEq(b, eq).let { true }

fun <A> IOOf<A>.equalUnderTheLaw(b: IOOf<A>, EQA: Eq<A> = Eq.any(), timeout: Duration = 20.seconds): Boolean =
  (this should object : Matcher<IOOf<A>> {
    override fun test(value: IOOf<A>): MatcherResult =
      arrow.core.Either.eq(Eq.any(), EQA).run {
        IO.applicative().mapN(value.fix().attempt(), b.fix().attempt()) { (a, b) ->
            MatcherResult(a.eqv(b), "Expected: $b but found: $a", "$b and $a should be equal")
          }
          .waitFor(timeout)
          .unsafeRunSync()
      }
  }).let { true }

fun <A> A.shouldBeEq(b: A, eq: Eq<A>) = this should matchUnderEq(eq, b)

fun <A> A.shouldNotBeEq(b: A, eq: Eq<A>) = this shouldNot matchUnderEq(eq, b)

fun <A> matchUnderEq(eq: Eq<A>, b: A) = object : Matcher<A> {
  override fun test(value: A): MatcherResult =
    MatcherResult(eq.run { value.eqv(b) }, "Expected: $b but found: $value", "$b and $value should be equal")
}
