package arrow.fx.internal

import arrow.Kind
import arrow.core.Either
import arrow.core.None
import arrow.core.Option
import arrow.core.Right
import arrow.core.Some
import arrow.core.Tuple2
import arrow.core.toT
import arrow.fx.Listener

import arrow.fx.MVar
import arrow.fx.internal.CancelableMVar.Companion.State.WaitForPut
import arrow.fx.internal.CancelableMVar.Companion.State.WaitForTake
import arrow.fx.typeclasses.CancelToken
import arrow.fx.typeclasses.Concurrent
import arrow.fx.typeclasses.Fiber
import arrow.fx.typeclasses.mapUnit
import arrow.fx.typeclasses.rightUnit
import kotlinx.atomicfu.atomic
import kotlin.coroutines.EmptyCoroutineContext

internal class CancelableMVar<F, A> private constructor(
  initial: State<A>,
  private val CF: Concurrent<F>
) : MVar<F, A>, Concurrent<F> by CF {

  private val state = atomic(initial)

  override fun isEmpty(): Kind<F, Boolean> = later {
    when (state.value) {
      is WaitForPut -> true
      is WaitForTake -> false
    }
  }

  override fun isNotEmpty(): Kind<F, Boolean> = later {
    when (state.value) {
      is WaitForPut -> false
      is WaitForTake -> true
    }
  }

  override fun put(a: A): Kind<F, Unit> =
    tryPut(a).flatMap { didPut ->
      if (didPut) unit() else cancelableF { cb -> unsafePut(a, cb) }
    }

  override fun tryPut(a: A): Kind<F, Boolean> =
    defer { unsafeTryPut(a) }

  override fun take(): Kind<F, A> =
    tryTake().flatMap {
      it.fold({ cancelableF(::unsafeTake) }, ::just)
    }

  override fun tryTake(): Kind<F, Option<A>> =
    defer { unsafeTryTake() }

  override fun read(): Kind<F, A> =
    cancelable(::unsafeRead)

  private tailrec fun unsafeTryPut(a: A): Kind<F, Boolean> =
    when (val current = state.value) {
      is WaitForTake -> justFalse
      is WaitForPut -> {
        var first: Listener<A>? = null
        val update: State<A> = if (current.takes.isEmpty()) {
          State(a)
        } else {
          val (x, rest) = current.takes.dequeue()
          first = x
          if (rest.isEmpty()) State.empty()
          else WaitForPut(LinkedMap.empty(), rest)
        }

        if (!state.compareAndSet(current, update)) {
          unsafeTryPut(a)
        } else if (first != null || current.reads.isNotEmpty()) {
          callPutAndAllReaders(a, first, current.reads)
        } else justTrue
      }
    }

  private tailrec fun unsafePut(a: A, onPut: Listener<Unit>): Kind<F, CancelToken<F>> =
    when (val current = state.value) {
      is WaitForTake -> {
        val id = Token()
        val newMap = current.listeners + (id toT Tuple2(a, onPut))
        if (state.compareAndSet(current, WaitForTake(current.value, newMap))) just(later { unsafeCancelPut(id) })
        else unsafePut(a, onPut)
      }
      is WaitForPut -> {
        var first: Listener<A>? = null
        val update = if (current.takes.isEmpty()) {
          State(a)
        } else {
          val (x, rest) = current.takes.dequeue()
          first = x
          if (rest.isEmpty()) State.empty()
          else WaitForPut(LinkedMap.empty(), rest)
        }

        if (state.compareAndSet(current, update)) {
          if (first != null || current.reads.isNotEmpty()) {
            callPutAndAllReaders(a, first, current.reads).map {
              onPut(rightUnit)
              unit()
            }
          } else {
            onPut(rightUnit)
            justUnit
          }
        } else unsafePut(a, onPut)
      }
    }

  private tailrec fun unsafeCancelPut(id: Token): Unit =
    when (val current = state.value) {
      is WaitForTake -> {
        val update = current.copy(listeners = current.listeners - id)
        if (state.compareAndSet(current, update)) Unit
        else unsafeCancelPut(id)
      }
      is WaitForPut -> Unit
    }

  private tailrec fun unsafeTryTake(): Kind<F, Option<A>> =
    when (val current = state.value) {
      is WaitForTake -> {
        if (current.listeners.isEmpty()) {
          if (state.compareAndSet(current, State.empty())) just(Some(current.value))
          else unsafeTryTake()
        } else {
          val (v, xs) = current.listeners.dequeue()
          val (ax, notify) = v
          val update = WaitForTake(ax, xs)
          if (state.compareAndSet(current, update)) {
            later { notify(rightUnit) }.fork(EmptyCoroutineContext).map { Some(current.value) }
          } else {
            unsafeTryTake()
          }
        }
      }
      is WaitForPut -> justNone
    }

  private tailrec fun unsafeTake(onTake: Listener<A>): Kind<F, CancelToken<F>> =
    when (val current = state.value) {
      is WaitForTake -> {
        if (current.listeners.isEmpty()) {
          if (state.compareAndSet(current, State.empty())) {
            onTake(Right(current.value))
            justUnit
          } else {
            unsafeTake(onTake)
          }
        } else {
          val (v, xs) = current.listeners.dequeue()
          val (ax, notify) = v
          if (state.compareAndSet(current, WaitForTake(ax, xs))) {
            later { notify(rightUnit) }.fork(EmptyCoroutineContext).map {
              onTake(Right(current.value))
              unit()
            }
          } else unsafeTake(onTake)
        }
      }
      is WaitForPut -> {
        val id = Token()
        val newQueue = current.takes + (id toT onTake)
        if (state.compareAndSet(current, WaitForPut(current.reads, newQueue))) just(later { unsafeCancelTake(id) })
        else unsafeTake(onTake)
      }
    }

  private tailrec fun unsafeCancelTake(id: Token): Unit =
    when (val current = state.value) {
      is WaitForPut -> {
        val newMap = current.takes - id
        val update = WaitForPut(current.reads, newMap)
        if (state.compareAndSet(current, update)) Unit
        else unsafeCancelTake(id)
      }
      is WaitForTake -> Unit
    }

  private tailrec fun unsafeRead(onRead: Listener<A>): Kind<F, Unit> =
    when (val current = state.value) {
      is WaitForTake -> {
        onRead(Right(current.value))
        unit()
      }
      is WaitForPut -> {
        val id = Token()
        val newReads = current.reads + (id toT onRead)
        if (state.compareAndSet(current, WaitForPut(newReads, current.takes))) later { unsafeCancelRead(id) }
        else unsafeRead(onRead)
      }
    }

  private tailrec fun unsafeCancelRead(id: Token): Unit =
    when (val current = state.value) {
      is WaitForPut -> {
        val newMap = current.reads - id
        val update = WaitForPut(newMap, current.takes)
        if (state.compareAndSet(current, update)) Unit
        else unsafeCancelRead(id)
      }
      is WaitForTake -> Unit
    }

  private fun callPutAndAllReaders(
    a: A,
    put: Listener<A>?,
    reads: LinkedMap<Token, Listener<A>>
  ): Kind<F, Boolean> {
    val value = Right(a)
    return reads.values.callAll(value).flatMap {
      if (put != null) later { put(value) }.fork(EmptyCoroutineContext).map { true }
      else justTrue
    }
  }

  // For streaming a value to a whole `reads` collection
  private fun Iterable<Listener<A>>.callAll(value: Either<Nothing, A>): Kind<F, Unit> =
    fold(null as Kind<F, Fiber<F, Unit>>?) { acc, cb ->
      val task = later { cb(value) }.fork(EmptyCoroutineContext)
      acc?.flatMap { task } ?: task
    }?.map(mapUnit) ?: unit()

  private val justNone = just(None)
  private val justFalse = just(false)
  private val justTrue = just(true)
  private val justUnit = just(unit())

  companion object {
    /** Builds an [UncancelableMVar] instance with an [initial] value. */
    operator fun <F, A> invoke(initial: A, CF: Concurrent<F>): Kind<F, MVar<F, A>> = CF.later {
      CancelableMVar(State(initial), CF)
    }

    /** Returns an empty [UncancelableMVar] instance. */
    fun <F, A> empty(CF: Concurrent<F>): Kind<F, MVar<F, A>> = CF.later {
      CancelableMVar(State.empty<A>(), CF)
    }

    internal sealed class State<out A> {
      companion object {
        private val ref = WaitForPut<Any>(LinkedMap.empty(), LinkedMap.empty())
        operator fun <A> invoke(a: A): State<A> = WaitForTake(a, LinkedMap.empty())
        @Suppress("UNCHECKED_CAST")
        fun <A> empty(): State<A> = ref as State<A>
      }

      data class WaitForPut<A>(val reads: LinkedMap<Token, Listener<A>>, val takes: LinkedMap<Token, Listener<A>>) : State<A>()
      data class WaitForTake<A>(val value: A, val listeners: LinkedMap<Token, Tuple2<A, Listener<Unit>>>) : State<A>()
    }
  }
}
