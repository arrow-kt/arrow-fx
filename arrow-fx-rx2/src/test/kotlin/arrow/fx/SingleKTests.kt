package arrow.fx

import arrow.Kind
import arrow.core.left
import arrow.fx.rx2.ForSingleK
import arrow.fx.rx2.SingleK
import arrow.fx.rx2.SingleKOf
import arrow.fx.test.eq.unsafeRunEq
import arrow.fx.rx2.extensions.concurrent
import arrow.fx.rx2.extensions.fx
import arrow.fx.rx2.extensions.singlek.applicative.applicative
import arrow.fx.rx2.extensions.singlek.applicativeError.attempt
import arrow.fx.rx2.extensions.singlek.async.async
import arrow.fx.rx2.extensions.singlek.functor.functor
import arrow.fx.rx2.extensions.singlek.monad.flatMap
import arrow.fx.rx2.extensions.singlek.monad.monad
import arrow.fx.rx2.extensions.singlek.timer.timer
import arrow.fx.rx2.fix
import arrow.fx.rx2.k
import arrow.fx.rx2.unsafeRunSync
import arrow.fx.rx2.value
import arrow.fx.typeclasses.ExitCase
import arrow.core.test.generators.GenK
import arrow.core.test.generators.throwable
import arrow.fx.test.laws.ConcurrentLaws
import arrow.fx.test.laws.forFew
import arrow.typeclasses.Eq
import arrow.typeclasses.EqK
import io.kotlintest.properties.Gen
import io.kotlintest.shouldBe
import io.reactivex.Single
import io.reactivex.observers.TestObserver
import io.reactivex.schedulers.Schedulers
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class SingleKTests : RxJavaSpec() {

  private val awaitDelay = 300L

  init {
    testLaws(
      ConcurrentLaws.laws(
        SingleK.concurrent(),
        SingleK.timer(),
        SingleK.functor(),
        SingleK.applicative(),
        SingleK.monad(),
        SingleK.genK(),
        SingleK.eqK(),
        testStackSafety = false
      )
    )

    "Multi-thread Singles finish correctly" {
      forFew(10, Gen.choose(10L, 50)) { delay ->
        SingleK.fx {
          val a = Single.timer(delay, TimeUnit.MILLISECONDS).k().bind()
          a
        }.value()
          .test()
          .awaitDone(delay + awaitDelay, TimeUnit.MILLISECONDS)
          .assertTerminated()
          .assertComplete()
          .assertNoErrors()
          .assertValue(0)
          .let { true }
      }
    }

    "Multi-thread Singles should run on their required threads" {
      forFew(10, Gen.choose(10L, 50)) { delay ->
        val originalThread: Thread = Thread.currentThread()
        var threadRef: Thread? = null

        val value: Single<Long> = SingleK.fx {
          val a = Single.timer(delay, TimeUnit.MILLISECONDS, Schedulers.io()).k().bind()
          threadRef = Thread.currentThread()
          val b = Single.just(a).observeOn(Schedulers.computation()).k().bind()
          b
        }.value()

        val test: TestObserver<Long> = value.test()
        val lastThread: Thread = test.awaitDone(delay + awaitDelay, TimeUnit.MILLISECONDS).lastThread()
        val nextThread = (threadRef?.name ?: "")

        nextThread != originalThread.name && lastThread.name != originalThread.name && lastThread.name != nextThread
      }
    }

    "Single dispose forces binding to cancel without completing too" {
      forFew(5, Gen.choose(10L, 50)) { delay ->
        val value: Single<Long> = SingleK.fx {
          val a = Single.timer(delay + awaitDelay, TimeUnit.MILLISECONDS).k().bind()
          a
        }.value()

        val test: TestObserver<Long> = value.doOnSubscribe { subscription ->
          Single.timer(delay, TimeUnit.MILLISECONDS).subscribe { _ ->
            subscription.dispose()
          }
        }.test()

        test.awaitTerminalEvent(delay + (2 * awaitDelay), TimeUnit.MILLISECONDS)
        test.assertNotTerminated()
          .assertNotComplete()
          .assertNoErrors()
          .assertNoValues()
          .let { true }
      }
    }

    "SingleK bracket cancellation should release resource with cancel exit status" {
      lateinit var ec: ExitCase<Throwable>
      val countDownLatch = CountDownLatch(1)

      SingleK.just(Unit)
        .bracketCase(
          use = { SingleK.async<Nothing> { } },
          release = { _, exitCase ->
            SingleK {
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

    "SingleK cancellable should cancel CancelToken on dispose" {
      Promise.uncancellable<ForSingleK, Unit>(SingleK.async()).flatMap { latch ->
        SingleK {
          SingleK.cancellable<Unit> {
            latch.complete(Unit)
          }.single.subscribe().dispose()
        }.flatMap { latch.get() }
      }.value()
        .test()
        .assertValue(Unit)
        .awaitTerminalEvent(100, TimeUnit.MILLISECONDS)
    }

    "SingleK async should be cancellable" {
      Promise.uncancellable<ForSingleK, Unit>(SingleK.async())
        .flatMap { latch ->
          SingleK {
            SingleK.async<Unit> { }
              .value()
              .doOnDispose { latch.complete(Unit).value().subscribe() }
              .subscribe()
              .dispose()
          }.flatMap { latch.get() }
        }.value()
        .test()
        .assertValue(Unit)
        .awaitTerminalEvent(100, TimeUnit.MILLISECONDS)
    }

    "SingleK should suspend" {
      fun getSingle(): Single<Int> = Single.just(1)

      SingleK.fx {
        val s = effect { getSingle().k().suspended() }.bind()

        s shouldBe 1
      }.unsafeRunSync()
    }

    "Error SingleK should suspend" {
      val error = IllegalArgumentException()

      SingleK.fx {
        val s = effect { Single.error<Int>(error).k().suspended() }.attempt().bind()

        s shouldBe error.left()
      }.unsafeRunSync()
    }
  }
}

private fun <A> Gen.Companion.singleK(gen: Gen<A>): Gen<SingleK<A>> =
  Gen.oneOf(
    gen.map { Single.just(it) },
    Gen.throwable().map { Single.error<A>(it) }
  ).map {
    it.k()
  }

private fun SingleK.Companion.genK() = object : GenK<ForSingleK> {
  override fun <A> genK(gen: Gen<A>): Gen<Kind<ForSingleK, A>> =
    Gen.singleK(gen) as Gen<Kind<ForSingleK, A>>
}

private fun <T> SingleK.Companion.eq(): Eq<SingleKOf<T>> = object : Eq<SingleKOf<T>> {
  override fun SingleKOf<T>.eqv(b: SingleKOf<T>): Boolean =
    unsafeRunEq(
      {
        this.value().timeout(5, TimeUnit.SECONDS).blockingGet()
      },
      {
        b.value().timeout(5, TimeUnit.SECONDS).blockingGet()
      }
    )
}

private fun SingleK.Companion.eqK() = object : EqK<ForSingleK> {
  override fun <A> Kind<ForSingleK, A>.eqK(other: Kind<ForSingleK, A>, EQ: Eq<A>): Boolean =
    (this.fix() to other.fix()).let {
      SingleK.eq<A>().run {
        it.first.eqv(it.second)
      }
    }
}
