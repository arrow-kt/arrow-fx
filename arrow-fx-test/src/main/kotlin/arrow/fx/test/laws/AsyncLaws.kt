package arrow.fx.test.laws

import arrow.Kind
import arrow.core.Either
import arrow.core.Left
import arrow.core.Right
import arrow.core.extensions.eq
import arrow.fx.internal.AtomicBooleanW
import arrow.core.test.generators.GenK
import arrow.core.test.generators.applicativeError
import arrow.core.test.generators.either
import arrow.core.test.generators.functionAToB
import arrow.core.test.generators.functionToA
import arrow.core.test.generators.intSmall
import arrow.core.test.generators.throwable
import arrow.core.test.laws.Law
import arrow.fx.Promise
import arrow.fx.typeclasses.Async
import arrow.fx.typeclasses.ExitCase
import arrow.typeclasses.Apply
import arrow.typeclasses.Eq
import arrow.typeclasses.EqK
import arrow.typeclasses.Functor
import arrow.typeclasses.Selective
import io.kotlintest.properties.Gen
import io.kotlintest.properties.forAll
import io.kotlintest.shouldBe
import kotlinx.coroutines.newSingleThreadContext

object AsyncLaws {

  private val one = newSingleThreadContext("1")
  private val two = newSingleThreadContext("2")

  private fun <F> asyncLaws(AC: Async<F>, GENK: GenK<F>, EQK: EqK<F>): List<Law> {
    val EQ = EQK.liftEq(Int.eq())
    val EQB = EQK.liftEq(Boolean.eq())
    val EQString = EQK.liftEq(String.eq())

    return listOf(
      Law("Async Laws: success equivalence") { AC.asyncSuccess(EQ) },
      Law("Async Laws: error equivalence") { AC.asyncError(EQ) },
      Law("Async Laws: continueOn jumps threads") { AC.continueOn(EQ) },
      Law("Async Laws: async constructor") { AC.asyncConstructor(EQ) },
      Law("Async Laws: async can be derived from asyncF") { AC.asyncCanBeDerivedFromAsyncF(EQ) },
      Law("Async Laws: bracket release is called on completed or error") { AC.bracketReleaseIscalledOnCompletedOrError(EQ) },
      Law("Async Laws: continueOn on comprehensions") { AC.continueOnComprehension(EQ) },
      Law("Async Laws: effect calls suspend functions in the right dispatcher") { AC.effectCanCallSuspend(EQ) },
      Law("Async Laws: effect is equivalent to later") { AC.effectEquivalence(EQ) },
      Law("Async Laws: fx block runs lazily") { AC.fxLazyEvaluation(Boolean.eq(), EQB) },
      Law("Async Laws: defer should be consistent with defer on provided coroutine context") { AC.derivedDefer(EQString) },
      Law("Async Laws: laterOrRaise should be consistent with laterOrRaise on provided coroutine context") { AC.derivedLaterOrRaise(EQ) },
      Law("Async Laws: continueOn should be consistent with continueOn on provided coroutine context") { AC.derivedContinueOn(EQ) },
      Law("Async Laws: shift should be consistent with shift given a coroutine context") { AC.derivedShift(EQ) },
      Law("Async Laws: effectMap constructs a suspend effect") { AC.effectMapSuspendEffect(GENK, EQ) }
    )
  }

  fun <F> laws(
    AC: Async<F>,
    GENK: GenK<F>,
    EQK: EqK<F>,
    testStackSafety: Boolean = true,
    iterations: Int = 5_000
  ): List<Law> =
    MonadDeferLaws.laws(AC, GENK, EQK, testStackSafety, iterations) +
      asyncLaws(AC, GENK, EQK)

  fun <F> laws(
    AC: Async<F>,
    FF: Functor<F>,
    AP: Apply<F>,
    SL: Selective<F>,
    GENK: GenK<F>,
    EQK: EqK<F>,
    testStackSafety: Boolean = true,
    iterations: Int = 5_000
  ): List<Law> =
    MonadDeferLaws.laws(AC, FF, AP, SL, GENK, EQK, testStackSafety, iterations) +
      asyncLaws(AC, GENK, EQK)

  fun <F> Async<F>.asyncSuccess(EQ: Eq<Kind<F, Int>>): Unit =
    forAll(50, Gen.int()) { num: Int ->
      async { ff: (Either<Throwable, Int>) -> Unit -> ff(Right(num)) }.equalUnderTheLaw(just(num), EQ)
    }

  fun <F> Async<F>.asyncError(EQ: Eq<Kind<F, Int>>): Unit =
    forAll(50, Gen.throwable()) { e: Throwable ->
      async { ff: (Either<Throwable, Int>) -> Unit -> ff(Left(e)) }.equalUnderTheLaw(raiseError(e), EQ)
    }

  fun <F> Async<F>.continueOn(EQ: Eq<Kind<F, Int>>): Unit =
    forFew(5, Gen.intSmall(), Gen.intSmall()) { threadId1: Int, threadId2: Int ->
      Unit.just()
        .continueOn(newSingleThreadContext(threadId1.toString()))
        .map { getCurrentThread() }
        .continueOn(newSingleThreadContext(threadId2.toString()))
        .map { it + getCurrentThread() }
        .equalUnderTheLaw(just(threadId1 + threadId2), EQ)
    }

  fun <F> Async<F>.asyncConstructor(EQ: Eq<Kind<F, Int>>): Unit =
    forFew(5, Gen.intSmall(), Gen.intSmall()) { threadId1: Int, threadId2: Int ->
      effect(newSingleThreadContext(threadId1.toString())) { getCurrentThread() }
        .flatMap {
          effect(newSingleThreadContext(threadId2.toString())) { it + getCurrentThread() }
        }
        .equalUnderTheLaw(just(threadId1 + threadId2), EQ)
    }

  fun <F> Async<F>.continueOnComprehension(EQ: Eq<Kind<F, Int>>): Unit =
    forFew(5, Gen.intSmall(), Gen.intSmall()) { threadId1: Int, threadId2: Int ->
      fx.async {
        continueOn(newSingleThreadContext(threadId1.toString()))
        val t1: Int = getCurrentThread()
        continueOn(newSingleThreadContext(threadId2.toString()))
        t1 + getCurrentThread()
      }.equalUnderTheLaw(just(threadId1 + threadId2), EQ)
    }

  fun <F> Async<F>.asyncCanBeDerivedFromAsyncF(EQ: Eq<Kind<F, Int>>): Unit =
    forAll(50, Gen.either(Gen.throwable(), Gen.int())) { eith ->
      val k: ((Either<Throwable, Int>) -> Unit) -> Unit = { f ->
        f(eith)
      }

      async(k).equalUnderTheLaw(asyncF { cb -> later { k(cb) } }, EQ)
    }

  fun <F> Async<F>.bracketReleaseIscalledOnCompletedOrError(EQ: Eq<Kind<F, Int>>) {
    forAll(50, Gen.string().applicativeError(this), Gen.int()) { fa, b ->
      Promise.uncancellable<F, Int>(this@bracketReleaseIscalledOnCompletedOrError).flatMap { promise ->
        val br = later { promise }.bracketCase(
          use = { fa },
          release = { r, exitCase ->
            when (exitCase) {
              is ExitCase.Completed -> r.complete(b)
              is ExitCase.Error -> r.complete(b)
              else -> just<Unit>(Unit)
            }
          }
        )

        asyncF<Unit> { cb -> later { cb(Right(Unit)) }.flatMap { br.attempt().mapConst(Unit) } }
          .flatMap { promise.get() }
      }.equalUnderTheLaw(just(b), EQ)
    }
  }

  fun <F> Async<F>.effectCanCallSuspend(EQ: Eq<Kind<F, Int>>): Unit =
    forAll(50, Gen.int()) { id ->
      val fs: suspend () -> Int = { id }

      effect { fs() }
        .equalUnderTheLaw(just(id), EQ)
    }

  fun <F> Async<F>.effectEquivalence(EQ: Eq<Kind<F, Int>>): Unit =
    forAll(50, Gen.functionAToB<Unit, Int>(Gen.constant(0))) { f ->
      val fs: suspend () -> Int = { f(Unit) }

      val effect = effect(one) { fs() }
      val continueOn = effect(two) { f(Unit) }

      effect.equalUnderTheLaw(continueOn, EQ)
    }

  fun <F> Async<F>.fxLazyEvaluation(EQ: Eq<Boolean>, EQK: Eq<Kind<F, Boolean>>) {
    val run = AtomicBooleanW(false)
    val p = fx.async {
      run.getAndSet(true)
      run.value
    }

    run.value.equalUnderTheLaw(false, EQ) shouldBe true
    p.equalUnderTheLaw(just(true), EQK) shouldBe true
  }

  fun <F> Async<F>.derivedDefer(EQK: Eq<Kind<F, String>>) {
    val f: () -> Kind<F, String> = { effect { Thread.currentThread().name } }
    defer(one, f).equalUnderTheLaw(just(Unit).continueOn(one).flatMap { defer(f) }, EQK)
  }

  fun <F> Async<F>.derivedLaterOrRaise(EQK: Eq<Kind<F, Int>>): Unit =
    forAll(50, Gen.functionToA(Gen.either(Gen.throwable(), Gen.int()))) { f: () -> Either<Throwable, Int> ->
      laterOrRaise(one, f).equalUnderTheLaw(defer(one) { f().fold({ raiseError<Int>(it) }, { just(it) }) }, EQK)
    }

  fun <F> Async<F>.derivedContinueOn(EQ: Eq<Kind<F, Int>>): Unit =
    forFew(5, Gen.intSmall()) { threadId1: Int ->
      val ctx = newSingleThreadContext(threadId1.toString())
      fx.async {
        continueOn(ctx)
        getCurrentThread()
      }
        .equalUnderTheLaw(fx.async { ctx.shift().bind(); getCurrentThread() }, EQ)
    }

  fun <F> Async<F>.derivedShift(EQ: Eq<Kind<F, Int>>): Unit =
    forFew(5, Gen.intSmall()) { threadId1: Int ->
      val ctx = newSingleThreadContext(threadId1.toString())
      ctx.shift().map { getCurrentThread() }
        .equalUnderTheLaw(ctx.run { effect(this) { getCurrentThread() } }, EQ)
    }

  fun <F> Async<F>.effectMapSuspendEffect(GK: GenK<F>, EQK: Eq<Kind<F, Int>>): Unit =
    forAll(50, GK.genK(Gen.int()), Gen.functionAToB<Int, Int>(Gen.int())) { fa: Kind<F, Int>, f: (Int) -> Int ->
      fa.effectMap { f(it) }.equalUnderTheLaw(fa.flatMap { a -> effect { f(a) } }, EQK)
    }

  // Turns out that kotlinx.coroutines decides to rewrite thread names
  private fun getCurrentThread() =
    Thread.currentThread().name.substringBefore(' ').toInt()
}
