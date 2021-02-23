package arrow.fx

import arrow.Kind
import arrow.core.test.generators.GenK
import arrow.core.test.laws.MonadFilterLaws
import arrow.core.test.laws.TraverseLaws
import arrow.fx.rx2.FlowableK
import arrow.fx.rx2.FlowableKOf
import arrow.fx.test.eq.unsafeRunEq
import arrow.fx.rx2.ForFlowableK
import arrow.fx.rx2.extensions.asyncDrop
import arrow.fx.rx2.extensions.asyncError
import arrow.fx.rx2.extensions.asyncLatest
import arrow.fx.rx2.extensions.asyncMissing
import arrow.fx.rx2.extensions.concurrent
import arrow.fx.rx2.extensions.flowablek.applicative.applicative
import arrow.fx.rx2.extensions.flowablek.async.async
import arrow.fx.rx2.extensions.flowablek.functor.functor
import arrow.fx.rx2.extensions.flowablek.monad.flatMap
import arrow.fx.rx2.extensions.flowablek.monad.monad
import arrow.fx.rx2.extensions.flowablek.monadFilter.monadFilter
import arrow.fx.rx2.extensions.flowablek.timer.timer
import arrow.fx.rx2.extensions.flowablek.traverse.traverse
import arrow.fx.rx2.extensions.fx
import arrow.fx.rx2.fix
import arrow.fx.rx2.k
import arrow.fx.rx2.value
import arrow.fx.typeclasses.ExitCase
import arrow.fx.test.laws.AsyncLaws
import arrow.fx.test.laws.ConcurrentLaws
import arrow.typeclasses.Eq
import arrow.typeclasses.EqK
import io.kotlintest.properties.Gen
import io.kotlintest.shouldBe
import io.reactivex.Flowable
import io.reactivex.subscribers.TestSubscriber
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class FlowableKTests : RxJavaSpec() {

  fun <T> EQ(): Eq<FlowableKOf<T>> = object : Eq<FlowableKOf<T>> {
    override fun FlowableKOf<T>.eqv(b: FlowableKOf<T>): Boolean =
      unsafeRunEq(
        {
          this.value().timeout(5, TimeUnit.SECONDS).blockingFirst()
        },
        {
          b.value().timeout(5, TimeUnit.SECONDS).blockingFirst()
        }
      )
  }

  fun EQK() = object : EqK<ForFlowableK> {
    override fun <A> Kind<ForFlowableK, A>.eqK(other: Kind<ForFlowableK, A>, EQ: Eq<A>): Boolean =
      EQ<A>().run {
        this@eqK.fix().eqv(other.fix())
      }
  }

  fun <A> GEN(gen: Gen<A>): Gen<FlowableK<A>> =
    Gen.list(gen).map {
      Flowable.fromIterable(it).k()
    }

  fun GENK() = object : GenK<ForFlowableK> {
    override fun <A> genK(gen: Gen<A>): Gen<Kind<ForFlowableK, A>> =
      GEN(gen) as Gen<Kind<ForFlowableK, A>>
  }

  init {
    testLaws(ConcurrentLaws.laws(FlowableK.concurrent(), FlowableK.timer(), FlowableK.functor(), FlowableK.applicative(), FlowableK.monad(), GENK(), EQK(), testStackSafety = false))
    // FIXME(paco) #691
    // testLaws(AsyncLaws.laws(FlowableK.async(), EQ(), EQ()))
    // testLaws(AsyncLaws.laws(FlowableK.async(), EQ(), EQ()))

    testLaws(AsyncLaws.laws(FlowableK.asyncDrop(), FlowableK.functor(), FlowableK.applicative(), FlowableK.monad(), GENK(), EQK(), testStackSafety = false))
    // FIXME(paco) #691
    // testLaws(AsyncLaws.laws(FlowableK.asyncDrop(), EQ(), EQ()))
    // testLaws(AsyncLaws.laws(FlowableK.asyncDrop(), EQ(), EQ()))

    testLaws(AsyncLaws.laws(FlowableK.asyncError(), FlowableK.functor(), FlowableK.applicative(), FlowableK.monad(), GENK(), EQK(), testStackSafety = false))
    // FIXME(paco) #691
    // testLaws(AsyncLaws.laws(FlowableK.asyncError(), EQ(), EQ()))
    // testLaws(AsyncLaws.laws(FlowableK.asyncError(), EQ(), EQ()))

    testLaws(AsyncLaws.laws(FlowableK.asyncLatest(), FlowableK.functor(), FlowableK.applicative(), FlowableK.monad(), GENK(), EQK(), testStackSafety = false))
    // FIXME(paco) #691
    // testLaws(AsyncLaws.laws(FlowableK.asyncLatest(), EQ(), EQ()))
    // testLaws(AsyncLaws.laws(FlowableK.asyncLatest(), EQ(), EQ()))

    testLaws(AsyncLaws.laws(FlowableK.asyncMissing(), FlowableK.functor(), FlowableK.applicative(), FlowableK.monad(), GENK(), EQK(), testStackSafety = false))
    // FIXME(paco) #691
    // testLaws(AsyncLaws.laws(FlowableK.asyncMissing(), EQ(), EQ()))
    // testLaws(AsyncLaws.laws(FlowableK.asyncMissing(), EQ(), EQ()))

    testLaws(TraverseLaws.laws(FlowableK.traverse(), GENK(), EQK()))

    testLaws(MonadFilterLaws.laws(FlowableK.monadFilter(), FlowableK.functor(), FlowableK.applicative(), FlowableK.monad(), GENK(), EQK()))

    "Multi-thread Flowables finish correctly" {
      val value: Flowable<Long> = FlowableK.fx {
        val a = Flowable.timer(2, TimeUnit.SECONDS).k().bind()
        a
      }.value()
      val test: TestSubscriber<Long> = value.test()
      test.awaitDone(5, TimeUnit.SECONDS)
      test.assertTerminated().assertComplete().assertNoErrors().assertValue(0)
    }

    "Flowable cancellation forces binding to cancel without completing too" {
      val value: Flowable<Long> = FlowableK.fx {
        val a = Flowable.timer(3, TimeUnit.SECONDS).k().bind()
        a
      }.value()
      val test: TestSubscriber<Long> = value.doOnSubscribe { subscription ->
        Flowable.timer(1, TimeUnit.SECONDS).subscribe {
          subscription.cancel()
        }
      }.test()
      test.awaitTerminalEvent(5, TimeUnit.SECONDS)
      test.assertNotTerminated().assertNotComplete().assertNoErrors().assertNoValues()
    }

    "FlowableK bracket cancellation should release resource with cancel exit status" {
      lateinit var ec: ExitCase<Throwable>
      val countDownLatch = CountDownLatch(1)

      FlowableK.just(Unit)
        .bracketCase(
          use = { FlowableK.async<Nothing>({ }) },
          release = { _, exitCase ->
            FlowableK {
              ec = exitCase
              countDownLatch.countDown()
            }
          }
        )
        .value()
        .subscribe()
        .dispose()

      countDownLatch.await(100, TimeUnit.MILLISECONDS)
      ec shouldBe ExitCase.Cancelled
    }

    "FlowableK cancellable should cancel CancelToken on dispose" {
      Promise.uncancellable<ForFlowableK, Unit>(FlowableK.async()).flatMap { latch ->
        FlowableK {
          FlowableK.cancellable<Unit>(
            fa = {
              latch.complete(Unit)
            }
          ).flowable.subscribe().dispose()
        }.flatMap { latch.get() }
      }.value()
        .test()
        .assertValue(Unit)
        .awaitTerminalEvent(100, TimeUnit.MILLISECONDS)
    }

    "FlowableK async should be cancellable" {
      Promise.uncancellable<ForFlowableK, Unit>(FlowableK.async())
        .flatMap { latch ->
          FlowableK {
            FlowableK.async<Unit>(fa = { })
              .value()
              .doOnCancel { latch.complete(Unit).value().subscribe() }
              .subscribe()
              .dispose()
          }.flatMap { latch.get() }
        }.value()
    }
  }
}
