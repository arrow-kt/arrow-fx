package arrow.fx.coroutines.stm

import arrow.fx.coroutines.AtomicRefW
import arrow.fx.coroutines.STMFrame
import arrow.fx.coroutines.STMTransaction
import kotlinx.atomicfu.AtomicInt
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.update
import kotlin.coroutines.resume

class TVar<A> constructor(a: A) {
  /**
   * The ref for a TVar stores either the STMFrame that currently locks the value or the value itself
   * This is used to implement locking. Reading threads have to loop until the value is released by a
   *  transaction.
   */
  private val ref: AtomicRefW<Any> = AtomicRefW(a as Any)

  /**
   * Each TVar has a unique id which is used to get a total ordering of variables to ensure that locks
   *  are always acquired in the same order on each thread
   */
  internal val id: Int = globalC.incrementAndGet()

  /**
   * A list of running transactions waiting for a change on this variable.
   * Changes are pushed to waiting transactions via [notify]
   */
  private val waiting = atomic<List<STMTransaction<*>>>(listOf())

  override fun hashCode(): Int = id
  override  fun equals(other: Any?): Boolean = this === other

  /**
   * Read the value of a [TVar]. This has no consistency guarantees for subsequent reads and writes
   *  since it is outside of a stm transaction.
   *
   * Much faster than `atomically { v.read() }` because it avoids creating a transaction, it just reads
   *  the value.
   */
  suspend fun unsafeRead(): A = read()

  /**
   * Internal unsafe (non-suspend) version of read. Used by various other internals and [unsafeRead] to
   *  read the current value respecting its state.
   */
  private fun read(): A {
    while (true) {
      ref.value.let {
        if (it !is STMFrame) return@read it as A
      }
    }
  }

  /**
   * Release a lock held by [frame].
   *
   * If [frame] no longer has the lock (a write happened and now read
   *  tries to unlock) it is ignored (By the semantics of [AtomicRefW.compareAndSet])
   */
  internal fun release(frame: STMFrame, a: A): Unit {
    ref.compareAndSet(frame, a as Any)
  }

  /**
   * Lock a [TVar] by replacing the value with [frame].
   *
   * This forces all further reads to wait until [frame] is done with the value.
   *
   * This works by continuously calling [read] and then trying to compare and set the frame.
   * If the value has been modified after reading it tries again, if the value inside is locked
   *  it will loop inside [read] until it is unlocked.
   */
  internal fun lock(frame: STMFrame): A {
    var res: A
    do {
      res = read()
    } while (ref.compareAndSet(res as Any, frame).not())
    return res
  }

  /**
   * Queue a transaction to be notified when this [TVar] is changed and [notify] is called.
   * This does not happen implicitly on [release] because release may also write the same value back on
   *  normal lock release.
   */
  internal fun queue(trans: STMTransaction<*>): Unit {
    waiting.update { it + trans }
  }

  /**
   * Resume execution of all transactions waiting for this [TVar] to change.
   */
  internal fun notify(): Unit {
    waiting.getAndSet(listOf()).forEach { it.getCont()?.resume(Unit) }
  }

  companion object {
    /**
     * Return a new [TVar]
     *
     * More efficient than `atomically { newVar(a) }` because it skips creating a transaction.
     */
    suspend fun <A> new(a: A): TVar<A> = TVar(a)
  }
}

/**
 * I just need a unique number here...
 *
 * This gets problematic when we reach the initial value again after 2^32 - 1 increments and there are
 *  still TVars around with low ids
 *
 * Idea: Keep a weak list of all TVars and when this case happens globally lock them and reassign
 *  ids starting from 0 again.
 */
internal val globalC: AtomicInt = atomic(0)
