package arrow.integrations.android

import androidx.lifecycle.Lifecycle
import androidx.lifecycle.Lifecycle.State
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.LifecycleRegistry
import arrow.core.test.UnitSpec
import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.ForkAndForget
import arrow.fx.coroutines.Promise
import arrow.fx.coroutines.guaranteeCase
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.compile
import io.kotlintest.properties.Gen
import io.kotlintest.properties.forAll
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.runBlocking

class ExtensionsKtTest : UnitSpec() {

  private val ctx = newSingleThreadContext("all")

  init {

    "interruptWhen should cancel correctly" {
      forAll(Gen.int()) { i ->
        runBlocking {
          val s = Stream.constant(i)
          val scope = TestLifecycleOwner()
          val p = Promise<ExitCase>()
          val start = Promise<Unit>()

          ForkAndForget {
            guaranteeCase(
              fa = {
                Stream.effect { start.complete(Unit) }
                  .flatMap { s }
                  .compile()
                  .drain()
              },
              finalizer = { ex -> p.complete(ex) }
            )
          }

          start.get()
          scope.cancel()
          p.get() == ExitCase.Cancelled
        }
      }
    }
  }
}

private class TestLifecycleOwner : LifecycleOwner {
  private val registry = LifecycleRegistry(this)

  init {
    registry.currentState = State.CREATED
  }

  override fun getLifecycle(): Lifecycle = registry

  fun cancel() {
    registry.currentState = State.DESTROYED
  }

  fun reanimate() {
    registry.currentState = State.CREATED
  }
}
