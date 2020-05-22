package arrow.integrations.android

import androidx.lifecycle.Lifecycle
import androidx.lifecycle.Lifecycle.State
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.LifecycleRegistry
import arrow.core.None
import arrow.core.Right
import arrow.core.Some
import arrow.core.extensions.eq
import arrow.core.internal.AtomicRefW
import arrow.core.test.UnitSpec
import arrow.core.test.generators.throwable
import arrow.core.test.laws.equalUnderTheLaw
import arrow.fx.IO
import arrow.fx.IOResult
import arrow.fx.extensions.fx
import arrow.fx.extensions.io.async.effectMap
import arrow.fx.flatMap
import arrow.fx.onCancel
import arrow.fx.test.eq.eqK
import arrow.fx.typeclasses.milliseconds
import arrow.fx.typeclasses.seconds
import io.kotlintest.fail
import io.kotlintest.properties.Gen
import io.kotlintest.properties.forAll
import io.kotlintest.shouldBe
import kotlinx.coroutines.newSingleThreadContext

class ExtensionsKtTest : UnitSpec() {

  private val ctx = newSingleThreadContext("all")
  private val eqK = IO.eqK<Nothing>()

  init {

    // --------------- unsafeRunScoped ---------------

    "should rethrow exceptions within run block with unsafeRunScoped" {
      forAll(Gen.throwable()) { e ->
        try {
          val scope = TestLifecycleOwner()

          val ioa = IO<Int> { throw e }

          ioa.unsafeRunScoped(scope) { result ->
            result.fold({ throw it }, { fail("") }, { fail("") })
          }
          fail("Should rethrow the exception")
        } catch (throwable: Throwable) {
          throwable == e
        }
      }
    }

    "unsafeRunScoped should cancel correctly" {
      forAll(Gen.int()) { i ->
        IO.fx<Nothing, Int> {
          val scope = TestLifecycleOwner()
          val promise = !Promise<Int>()
          !IO.effect {
            IO.cancellable<Nothing, Unit> { promise.complete(i) }.unsafeRunScoped(scope) { }
          }
          !IO.effect { scope.cancel() }
          !promise.get()
        }.equalUnderTheLaw(IO.just(i), IO.eqK<Nothing>(timeout = 500.milliseconds).liftEq(Int.eq()))
      }
    }

    "unsafeRunScoped can cancel even for infinite asyncs" {
      IO.fx<Nothing, Int> {
        val scope = TestLifecycleOwner()
        val promise = !Promise<Int>()
        !IO.effect {
          IO(ctx) { -1 }.flatMap { IO.never }.onCancel(promise.complete(1)).unsafeRunScoped(scope) { }
        }
        !IO.sleep(500.milliseconds).effectMap { scope.cancel() }
        !promise.get()
      }.unsafeRunTimed(2.seconds) shouldBe Some(Right(1))
    }

    "should complete when running a pure value with unsafeRunScoped" {
      forAll(Gen.int()) { i ->
        val scope = TestLifecycleOwner()
        IO.async<Nothing, Int> { cb ->
          IO.just(i).unsafeRunScoped(scope) { result ->
            result.fold({ fail("") }, { fail("") }, { cb(IOResult.Success(it)) })
          }
        }.equalUnderTheLaw(IO.just(i), eqK.liftEq(Int.eq()))
      }
    }

    "unsafeRunScoped doesn't start if scope is cancelled" {
      forAll(Gen.int()) { i ->
        val scope = TestLifecycleOwner()
        val ref = AtomicRefW<Int?>(i)
        scope.cancel()
        IO { ref.value = null }.unsafeRunScoped(scope) {}
        ref.value == i
      }
    }

    // --------------- deferUntilActive ---------------

    "deferUntilActive waits until next onCreate if already destroyed" {
      forAll(Gen.int()) { i ->
        IO.fx<Boolean> {
          val scope = TestLifecycleOwner(State.DESTROYED)
          val mVar = MVar<Int>().bind()

          val a = mVar.put(i)
            .deferUntilActive(ctx, scope).fork().bind()

          mVar.tryTake().map { it is None }.bind()
          IO { scope.reanimate() }.bind()

          a.join().bind()
          mVar.take().map { it == i }.bind()
        }.equalUnderTheLaw(IO.just(true), eqK.liftEq(Boolean.eq()))
      }
    }

    "deferUntilActive waits until next onCreate" {
      forAll(Gen.int()) { i ->
        IO.fx<Boolean> {
          val scope = TestLifecycleOwner()
          val mv1 = !MVar<Unit>()
          val mv2 = !MVar<Int>()
          val a = mv1.take().followedBy(mv2.put(i))
            .deferUntilActive(ctx, scope).fork().bind()

          IO { scope.cancel() }.bind()

          mv1.put(Unit).bind()
          mv2.tryTake().map { it is None }.bind()

          IO { scope.reanimate() }.bind()
          a.join()

          val res = mv2.take().map { it == i }.bind()

          res
        }.equalUnderTheLaw(IO.just(true), eqK.liftEq(Boolean.eq()))
      }
    }
  }
}

private class TestLifecycleOwner(
  state: State = State.CREATED
) : LifecycleOwner {
  private val registry = LifecycleRegistry(this)

  init {
    registry.currentState = state
  }

  override fun getLifecycle(): Lifecycle = registry

  fun cancel() {
    registry.currentState = State.DESTROYED
  }

  fun reanimate() {
    registry.currentState = State.CREATED
  }
}
