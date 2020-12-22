package arrow.fx.test.laws

import arrow.Kind
import arrow.core.extensions.eq
import arrow.core.test.generators.GenK
import arrow.core.test.generators.fatalThrowable
import arrow.core.test.generators.throwable
import arrow.core.test.laws.Law
import arrow.core.test.laws.MonadErrorLaws
import arrow.fx.test.eq.throwableEq
import arrow.typeclasses.Apply
import arrow.typeclasses.Eq
import arrow.typeclasses.EqK
import arrow.typeclasses.Functor
import arrow.typeclasses.MonadThrow
import arrow.typeclasses.Selective
import io.kotlintest.fail
import io.kotlintest.properties.Gen
import io.kotlintest.properties.forAll
import io.kotlintest.shouldThrowAny

object MonadThrowLaws {

  private fun <F> monadThrowLaws(M: MonadThrow<F>, EQK: EqK<F>): List<Law> {
    val EQ = EQK.liftEq(Int.eq())
    val EQThrowable: Eq<Kind<F, Kind<F, Throwable>>> = EQK.liftEq(EQK.liftEq(throwableEq()))

    return listOf(
      Law("Monad Throw Laws: raises non fatal") { M.monadRaiseNonFatalRaiseError(EQ) },
      Law("Monad Throw Laws: throws error") { M.monadRaiseNonFatalThrowsError(EQThrowable) }
    )
  }

  fun <F> laws(M: MonadThrow<F>, GENK: GenK<F>, EQK: EqK<F>): List<Law> =
    MonadErrorLaws.laws(M, GENK, EQK) +
      monadThrowLaws(M, EQK)

  fun <F> laws(
    M: MonadThrow<F>,
    FF: Functor<F>,
    AP: Apply<F>,
    SL: Selective<F>,
    GENK: GenK<F>,
    EQK: EqK<F>
  ): List<Law> =
    MonadErrorLaws.laws(M, FF, AP, SL, GENK, EQK) +
      monadThrowLaws(M, EQK)

  fun <F> MonadThrow<F>.monadRaiseNonFatalRaiseError(EQ: Eq<Kind<F, Int>>): Unit =
    forAll(50, Gen.throwable()) { e: Throwable ->
      e.raiseNonFatal<Int>().equalUnderTheLaw(raiseError(e), EQ)
    }

  fun <F> MonadThrow<F>.monadRaiseNonFatalThrowsError(EQ: Eq<Kind<F, Kind<F, Throwable>>>): Unit =
    forAll(50, Gen.fatalThrowable()) { fatal: Throwable ->
      shouldThrowAny {
        fun itShouldNotComeThisFar(): Kind<F, Throwable> {
          fail("MonadThrow should rethrow the fatal Throwable: '$fatal'.")
        }

        catch { fatal.raiseNonFatal<Throwable>() }.equalUnderTheLaw(just(itShouldNotComeThisFar()), EQ)
      } == fatal
    }
}
