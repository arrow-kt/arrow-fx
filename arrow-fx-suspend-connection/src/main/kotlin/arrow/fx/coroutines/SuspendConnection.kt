package arrow.fx.coroutines

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.Continuation
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.startCoroutine

fun CoroutineContext.connection(): SuspendConnection =
  this[SuspendConnection] ?: SuspendConnection.uncancellable

/**
 * SuspendConnection is a state-machine inside [CoroutineContext] that manages cancellation.
 * This could in the future also serve as a mechanism to collect debug information on running connections.
 */
sealed class SuspendConnection : AbstractCoroutineContextElement(SuspendConnection) {

  abstract suspend fun cancel(): Unit

  abstract fun isCancelled(): Boolean
  abstract fun push(tokens: List<suspend () -> Unit>): Unit
  fun isNotCancelled(): Boolean = !isCancelled()

  abstract fun push(token: suspend () -> Unit): Unit

  fun pushPair(lh: SuspendConnection, rh: SuspendConnection): Unit =
    pushPair({ lh.cancel() }, { rh.cancel() })

  fun pushPair(lh: suspend () -> Unit, rh: suspend () -> Unit): Unit =
    push(listOf(lh, rh))

  abstract fun pop(): suspend () -> Unit
  abstract fun tryReactivate(): Boolean

  companion object Key : CoroutineContext.Key<SuspendConnection> {
    val uncancellable: SuspendConnection = Uncancellable
    operator fun invoke(): SuspendConnection = DefaultConnection()
  }

  object Uncancellable : SuspendConnection() {
    override suspend fun cancel() = Unit
    override fun isCancelled(): Boolean = false
    override fun push(tokens: List<suspend () -> Unit>) = Unit
    override fun push(token: suspend () -> Unit) = Unit
    override fun pop(): suspend () -> Unit = suspend { Unit }
    override fun tryReactivate(): Boolean = true
    override fun toString(): String = "UncancellableConnection"
  }

  class DefaultConnection : SuspendConnection() {
    private val state: AtomicRef<List<suspend () -> Unit>?> = atomic(emptyList())

    override suspend fun cancel(): Unit =
      state.getAndSet(null).let { stack ->
        when {
          stack == null || stack.isEmpty() -> Unit
          else -> stack.cancelAll()
        }
      }

    override fun isCancelled(): Boolean = state.value == null

    override tailrec fun push(token: suspend () -> Unit): Unit = when (val list = state.value) {
      // If connection is already cancelled cancel token immediately.
      null -> token
        .startCoroutine(Continuation(EmptyCoroutineContext) { })
      else ->
        if (state.compareAndSet(list, listOf(token) + list)) Unit
        else push(token)
    }

    override fun push(tokens: List<suspend () -> Unit>) =
      push { tokens.cancelAll() }

    override tailrec fun pop(): suspend () -> Unit {
      val state = state.value
      return when {
        state == null || state.isEmpty() -> suspend { Unit }
        else ->
          if (this.state.compareAndSet(state, state.drop(1))) state.first()
          else pop()
      }
    }

    override fun tryReactivate(): Boolean =
      state.compareAndSet(null, emptyList())

    private suspend fun List<suspend () -> Unit>.cancelAll(): Unit =
      forEach { it.invoke() }

    override fun toString(): String =
      "SuspendConnection(isCancelled = ${isCancelled()}, size= ${state.value?.size ?: 0})"
  }
}
