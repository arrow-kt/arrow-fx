package arrow.fx.test.laws

import arrow.Kind
import arrow.core.extensions.eq
import arrow.core.extensions.list.foldable.foldLeft
import arrow.core.k
import arrow.core.left
import arrow.core.right
import arrow.core.test.concurrency.SideEffect
import arrow.core.test.generators.GenK
import arrow.core.test.generators.intSmall
import arrow.core.test.generators.throwable
import arrow.core.test.laws.Law
import arrow.fx.typeclasses.MonadDefer
import arrow.typeclasses.Apply
import arrow.typeclasses.Eq
import arrow.typeclasses.EqK
import arrow.typeclasses.Functor
import arrow.typeclasses.Selective
import io.kotlintest.properties.Gen
import io.kotlintest.properties.forAll
import io.kotlintest.shouldBe

object MonadDeferLaws {

  private fun <F> monadDeferLaws(
    SC: MonadDefer<F>,
    GENK: GenK<F>,
    EQK: EqK<F>,
    testStackSafety: Boolean = true,
    iterations: Int = 20_000
  ): List<Law> {
    val EQ = EQK.liftEq(Int.eq())

    return listOf(
      Law("MonadDefer laws: later constant equals pure") { SC.laterConstantEqualsPure(EQ) },
      Law("MonadDefer laws: later throw equals raiseError") { SC.laterThrowEqualsRaiseError(EQ) },
      Law("MonadDefer laws: later constant equals pure") { SC.deferConstantEqualsPure(EQ) },
      Law("MonadDefer laws: laterOrRaise constant right equals pure") { SC.laterOrRaiseConstantRightEqualsPure(EQ) },
      Law("MonadDefer laws: laterOrRaise constant left equals raiseError") { SC.laterOrRaiseConstantLeftEqualsRaiseError(EQ) },
      Law("MonadDefer laws: propagate error through bind") { SC.propagateErrorsThroughBind(EQ) },
      Law("MonadDefer laws: defer suspends evaluation") { SC.deferSuspendsEvaluation(EQ) },
      Law("MonadDefer laws: later suspends evaluation") { SC.delaySuspendsEvaluation(EQ) },
      Law("MonadDefer laws: flatMap suspends evaluation") { SC.flatMapSuspendsEvaluation(EQ) },
      Law("MonadDefer laws: map suspends evaluation") { SC.mapSuspendsEvaluation(EQ) },
      Law("MonadDefer laws: Repeated evaluation not memoized") { SC.repeatedSyncEvaluationNotMemoized(EQ) },
      Law("MonadDefer laws: later should be consistent") { SC.derivedLaterConsistent(GENK, EQ) },
      Law("MonadDefer laws: lazy should be consistent") { SC.derivedLazyConsistent(GENK, EQ) }
    ) + if (testStackSafety) {
      listOf(
        Law("MonadDefer laws: stack safety over repeated left binds") { SC.stackSafetyOverRepeatedLeftBinds(iterations, EQ) },
        Law("MonadDefer laws: stack safety over repeated right binds") { SC.stackSafetyOverRepeatedRightBinds(iterations, EQ) },
        Law("MonadDefer laws: stack safety over repeated attempts") { SC.stackSafetyOverRepeatedAttempts(iterations, EQ) },
        Law("MonadDefer laws: stack safety over repeated maps") { SC.stackSafetyOnRepeatedMaps(iterations, EQ) }
      )
    } else {
      emptyList()
    }
  }

  fun <F> laws(
    SC: MonadDefer<F>,
    GENK: GenK<F>,
    EQK: EqK<F>,
    testStackSafety: Boolean = true,
    iterations: Int = 5_000
  ): List<Law> =
    BracketLaws.laws(SC, GENK, EQK, testStackSafety, iterations) +
      MonadThrowLaws.laws(SC, GENK, EQK) +
      monadDeferLaws(SC, GENK, EQK, testStackSafety, iterations)

  fun <F> laws(
    SC: MonadDefer<F>,
    FF: Functor<F>,
    AP: Apply<F>,
    SL: Selective<F>,
    GENK: GenK<F>,
    EQK: EqK<F>,
    testStackSafety: Boolean = true,
    iterations: Int = 5_000
  ): List<Law> =
    BracketLaws.laws(SC, FF, AP, SL, GENK, EQK, testStackSafety, iterations) +
      MonadThrowLaws.laws(SC, SC, SC, SC, GENK, EQK) +
      monadDeferLaws(SC, GENK, EQK, testStackSafety, iterations)

  fun <F> MonadDefer<F>.derivedLaterConsistent(GK: GenK<F>, EQ: Eq<Kind<F, Int>>) {
    forAll(50, GK.genK(Gen.int()), Gen.intSmall()) { fa: Kind<F, Int>, x: Int ->
      later(fa).equalUnderTheLaw(defer { fa }, EQ)
    }
  }

  fun <F> MonadDefer<F>.derivedLazyConsistent(GK: GenK<F>, EQ: Eq<Kind<F, Int>>) {
    forAll(50, GK.genK(Gen.int())) { fa: Kind<F, Int> ->
      lazy().flatMap { fa }.equalUnderTheLaw(later { }.flatMap { fa }, EQ)
    }
  }

  fun <F> MonadDefer<F>.laterConstantEqualsPure(EQ: Eq<Kind<F, Int>>) {
    forAll(50, Gen.intSmall()) { x ->
      later { x }.equalUnderTheLaw(just(x), EQ)
    }
  }

  fun <F> MonadDefer<F>.deferConstantEqualsPure(EQ: Eq<Kind<F, Int>>) {
    forAll(50, Gen.intSmall()) { x ->
      defer { just(x) }.equalUnderTheLaw(just(x), EQ)
    }
  }

  fun <F> MonadDefer<F>.laterOrRaiseConstantRightEqualsPure(EQ: Eq<Kind<F, Int>>) {
    forAll(50, Gen.intSmall()) { x ->
      laterOrRaise { x.right() }.equalUnderTheLaw(just(x), EQ)
    }
  }

  fun <F> MonadDefer<F>.laterOrRaiseConstantLeftEqualsRaiseError(EQERR: Eq<Kind<F, Int>>) {
    forFew(5, Gen.throwable()) { t ->
      laterOrRaise { t.left() }.equalUnderTheLaw(raiseError(t), EQERR)
    }
  }

  fun <F> MonadDefer<F>.laterThrowEqualsRaiseError(EQERR: Eq<Kind<F, Int>>) {
    forFew(5, Gen.throwable()) { t ->
      later { throw t }.equalUnderTheLaw(raiseError(t), EQERR)
    }
  }

  fun <F> MonadDefer<F>.propagateErrorsThroughBind(EQERR: Eq<Kind<F, Int>>) {
    forFew(5, Gen.throwable()) { t ->
      later { throw t }.flatMap<Int, Int> { a: Int -> just(a) }.equalUnderTheLaw(raiseError(t), EQERR)
    }
  }

  fun <F> MonadDefer<F>.deferSuspendsEvaluation(EQ: Eq<Kind<F, Int>>) {
    val sideEffect = SideEffect(counter = 0)
    val df = defer { sideEffect.increment(); just(sideEffect.counter) }

    Thread.sleep(10)

    sideEffect.counter shouldBe 0
    df.equalUnderTheLaw(just(1), EQ) shouldBe true
  }

  fun <F> MonadDefer<F>.delaySuspendsEvaluation(EQ: Eq<Kind<F, Int>>) {
    val sideEffect = SideEffect(counter = 0)
    val df = later { sideEffect.increment(); sideEffect.counter }

    Thread.sleep(10)

    sideEffect.counter shouldBe 0
    df.equalUnderTheLaw(just(1), EQ) shouldBe true
  }

  fun <F> MonadDefer<F>.flatMapSuspendsEvaluation(EQ: Eq<Kind<F, Int>>) {
    val sideEffect = SideEffect(counter = 0)
    val df = just(0).flatMap { sideEffect.increment(); just(sideEffect.counter) }

    Thread.sleep(10)

    sideEffect.counter shouldBe 0
    df.equalUnderTheLaw(just(1), EQ) shouldBe true
  }

  fun <F> MonadDefer<F>.mapSuspendsEvaluation(EQ: Eq<Kind<F, Int>>) {
    val sideEffect = SideEffect(counter = 0)
    val df = just(0).map { sideEffect.increment(); sideEffect.counter }

    Thread.sleep(10)

    sideEffect.counter shouldBe 0
    df.equalUnderTheLaw(just(1), EQ) shouldBe true
  }

  fun <F> MonadDefer<F>.repeatedSyncEvaluationNotMemoized(EQ: Eq<Kind<F, Int>>) {
    val sideEffect = SideEffect()
    val df = later { sideEffect.increment(); sideEffect.counter }

    df.flatMap { df }.flatMap { df }.equalUnderTheLaw(just(3), EQ) shouldBe true
  }

  fun <F> MonadDefer<F>.stackSafetyOverRepeatedLeftBinds(iterations: Int = 20_000, EQ: Eq<Kind<F, Int>>): Unit =
    forAll(50, Gen.create { Unit }) {
      (0..iterations).toList().k().foldLeft(just(0)) { def, x ->
        def.flatMap { just(x) }
      }.equalUnderTheLaw(just(iterations), EQ)
    }

  fun <F> MonadDefer<F>.stackSafetyOverRepeatedRightBinds(iterations: Int = 20_000, EQ: Eq<Kind<F, Int>>): Unit =
    forAll(50, Gen.create { Unit }) {
      (0..iterations).toList().foldRight(just(iterations)) { x, def ->
        lazy().flatMap { def }
      }.equalUnderTheLaw(just(iterations), EQ)
    }

  fun <F> MonadDefer<F>.stackSafetyOverRepeatedAttempts(iterations: Int = 20_000, EQ: Eq<Kind<F, Int>>): Unit =
    forAll(50, Gen.create { Unit }) {
      (0..iterations).toList().foldLeft(just(0)) { def, x ->
        def.attempt().map { x }
      }.equalUnderTheLaw(just(iterations), EQ)
    }

  fun <F> MonadDefer<F>.stackSafetyOnRepeatedMaps(iterations: Int = 20_000, EQ: Eq<Kind<F, Int>>): Unit =
    forAll(50, Gen.create { Unit }) {
      (0..iterations).toList().foldLeft(just(0)) { def, x ->
        def.map { x }
      }.equalUnderTheLaw(just(iterations), EQ)
    }
}
