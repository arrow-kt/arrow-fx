package arrow.fx.reactor.extensions

import arrow.fx.internal.Platform
import kotlinx.atomicfu.atomic
import reactor.core.Disposable

internal class ForwardDisposable {

  private val state = atomic(init)

  fun cancel(): Disposable {
    fun loop(d: Disposable): Unit = state.value.let { current ->
      when(current) {
        is State.Empty -> if(!state.compareAndSet(current, State.Empty(listOf(d) + current.stack))) loop(d)
        is State.Active -> {
          state.lazySet(finished)
          Platform.trampoline { current.token.dispose() }
        }
      }
    }

    return object : Disposable {
      override fun dispose() =
        loop(this)
    }
  }

  fun complete(value: Disposable): Unit = state.value.let { current ->
    when(current) {
      is State.Active -> {
        value.dispose()
        throw IllegalStateException(current.toString())
      }
      is State.Empty -> if (current == init) {
        // If `init`, then `cancel` was not triggered yet
        if (!state.compareAndSet(current, State.Active(value)))
          complete(value)
      } else {
        if (!state.compareAndSet(current, finished))
          complete(value)
        else
          execute(value, current.stack)
      }
    }
  }

  companion object {
    private sealed class State {
      data class Empty(val stack: List<Disposable>) : State()
      data class Active(val token: Disposable) : State()
    }

    private val init: State = State.Empty(listOf())
    private val finished: State = State.Active(Disposable { })

    private fun execute(token: Disposable, stack: List<Disposable>): Unit =
      Platform.trampoline {
        token.dispose()
        stack.forEach { it.dispose() }
      }
  }
}
