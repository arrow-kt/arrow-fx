package arrow.fx

import arrow.Kind
import arrow.core.left
import arrow.fx.rx2.ForMaybeK
import arrow.fx.rx2.MaybeK
import arrow.fx.rx2.MaybeKOf
import arrow.fx.rx2.extensions.concurrent
import arrow.fx.rx2.extensions.fx
import arrow.fx.rx2.extensions.maybek.applicative.applicative
import arrow.fx.rx2.extensions.maybek.async.async
import arrow.fx.rx2.extensions.maybek.functor.functor
import arrow.fx.rx2.extensions.maybek.monad.flatMap
import arrow.fx.rx2.extensions.maybek.monad.monad
import arrow.fx.rx2.extensions.maybek.timer.timer
import arrow.fx.rx2.fix
import arrow.fx.rx2.k
import arrow.fx.rx2.unsafeRunSync
import arrow.fx.rx2.value
import arrow.fx.typeclasses.ExitCase
import arrow.core.test.generators.GenK
import arrow.core.test.generators.throwable
import arrow.fx.test.eq.unsafeRunEq
import arrow.fx.test.laws.ConcurrentLaws
import arrow.typeclasses.Eq
import arrow.typeclasses.EqK
import io.kotlintest.properties.Gen
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.reactivex.Maybe
import io.reactivex.observers.TestObserver
import io.reactivex.schedulers.Schedulers
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class MaybeKTests : RxJavaSpec() {

  init {
    testLaws(
      ConcurrentLaws.laws(
        MaybeK.concurrent(),
        MaybeK.timer(),
        MaybeK.functor(),
        MaybeK.applicative(),
        MaybeK.monad(),
        MaybeK.genk(),
        MaybeK.eqK(),
        testStackSafety = false
      )

      /*
      TODO: MonadFilter instances are not lawsful
      https://github.com/arrow-kt/arrow/issues/1881

      MonadFilterLaws.laws(
        MaybeK.monadFilter(),
        MaybeK.functor(),
        MaybeK.applicative(),
        MaybeK.monad(),
        MaybeK.genk(),
        MaybeK.eqK()
      )
       */
    )

    "Multi-thread Maybes finish correctly" {
      val value: Maybe<Long> = MaybeK.fx {
        val a = Maybe.timer(2, TimeUnit.SECONDS).k().bind()
        a
      }.value()

      val test: TestObserver<Long> = value.test()
      test.awaitDone(5, TimeUnit.SECONDS)
      test.assertTerminated().assertComplete().assertNoErrors().assertValue(0)
    }

    "Multi-thread Maybes should run on their required threads" {
      val originalThread: Thread = Thread.currentThread()
      var threadRef: Thread? = null

      val value: Maybe<Long> = MaybeK.fx {
        val a = Maybe.timer(2, TimeUnit.SECONDS, Schedulers.newThread()).k().bind()
        threadRef = Thread.currentThread()
        val b = Maybe.just(a).observeOn(Schedulers.newThread()).k().bind()
        b
      }.value()

      val test: TestObserver<Long> = value.test()
      val lastThread: Thread = test.awaitDone(5, TimeUnit.SECONDS).lastThread()
      val nextThread = (threadRef?.name ?: "")

      nextThread shouldNotBe originalThread.name
      lastThread.name shouldNotBe originalThread.name
      lastThread.name shouldNotBe nextThread
    }

    "Maybe dispose forces binding to cancel without completing too" {
      val value: Maybe<Long> = MaybeK.fx {
        val a = Maybe.timer(3, TimeUnit.SECONDS).k().bind()
        a
      }.value()

      val test: TestObserver<Long> = value.doOnSubscribe { subscription ->
        Maybe.timer(1, TimeUnit.SECONDS).subscribe { _ ->
          subscription.dispose()
        }
      }.test()

      test.awaitTerminalEvent(5, TimeUnit.SECONDS)
      test.assertNotTerminated().assertNotComplete().assertNoErrors().assertNoValues()
    }

    "Folding over empty Maybe runs ifEmpty lambda" {
      val emptyMaybe = Maybe.empty<String>().k()
      val foldedToEmpty = emptyMaybe.fold({ true }, { false })
      foldedToEmpty shouldBe true
    }

    "Folding over non-empty Maybe runs ifSome lambda" {
      val maybe = Maybe.just(1).k()
      val foldedToSome = maybe.fold({ false }, { true })
      foldedToSome shouldBe true
    }

    "MaybeK bracket cancellation should release resource with cancel exit status" {
      lateinit var ec: ExitCase<Throwable>
      val countDownLatch = CountDownLatch(1)
      MaybeK.just(Unit)
        .bracketCase(
          use = { MaybeK.async<Nothing> { } },
          release = { _, exitCase ->
            MaybeK {
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

    "MaybeK cancellable should cancel CancelToken on dispose" {
      Promise.uncancellable<ForMaybeK, Unit>(MaybeK.async()).flatMap { latch ->
        MaybeK {
          MaybeK.cancellable<Unit> {
            latch.complete(Unit)
          }.maybe.subscribe().dispose()
        }.flatMap { latch.get() }
      }.value()
        .test()
        .assertValue(Unit)
        .awaitTerminalEvent(100, TimeUnit.MILLISECONDS)
    }

    "MaybeK async should be cancellable" {
      Promise.uncancellable<ForMaybeK, Unit>(MaybeK.async())
        .flatMap { latch ->
          MaybeK {
            MaybeK.async<Unit> { }
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

    "MaybeK should suspend" {
      MaybeK.fx {
        val s = effect { Maybe.just(1).k().suspended() }.bind()

        s shouldBe 1
      }.unsafeRunSync()
    }

    "Error MaybeK should suspend" {
      val error = IllegalArgumentException()

      MaybeK.fx {
        val s = effect { Maybe.error<Int>(error).k().suspended() }.attempt().bind()

        s shouldBe error.left()
      }.unsafeRunSync()
    }

    "Empty MaybeK should suspend" {
      MaybeK.fx {
        val s = effect { Maybe.empty<Int>().k().suspended() }.bind()

        s shouldBe null
      }.unsafeRunSync()
    }
  }
}

private fun <T> MaybeK.Companion.eq(): Eq<MaybeKOf<T>> = object : Eq<MaybeKOf<T>> {
  override fun MaybeKOf<T>.eqv(b: MaybeKOf<T>): Boolean =
    unsafeRunEq(
      {
        this.value().timeout(5, TimeUnit.SECONDS).blockingGet()
      },
      {
        b.value().timeout(5, TimeUnit.SECONDS).blockingGet()
      }
    )
}

private fun MaybeK.Companion.eqK() = object : EqK<ForMaybeK> {
  override fun <A> Kind<ForMaybeK, A>.eqK(other: Kind<ForMaybeK, A>, EQ: Eq<A>): Boolean =
    (this.fix() to other.fix()).let {
      MaybeK.eq<A>().run {
        it.first.eqv(it.second)
      }
    }
}

private fun MaybeK.Companion.genk() = object : GenK<ForMaybeK> {
  override fun <A> genK(gen: Gen<A>): Gen<Kind<ForMaybeK, A>> =
    Gen.oneOf(
      Gen.constant(Maybe.empty<A>()),

      gen.map {
        Maybe.just(it)
      },

      Gen.throwable().map {
        Maybe.error<A>(it)
      }
    ).map {
      it.k()
    }
}
