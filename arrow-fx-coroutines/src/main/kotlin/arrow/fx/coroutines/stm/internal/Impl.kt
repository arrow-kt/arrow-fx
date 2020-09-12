package arrow.fx.coroutines.stm.internal

import arrow.fx.coroutines.STM
import arrow.fx.coroutines.stm.TVar
import kotlinx.atomicfu.atomic
import kotlin.coroutines.Continuation
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.startCoroutineUninterceptedOrReturn
import kotlin.coroutines.suspendCoroutine

/**
 * A STMFrame keeps the reads and writes performed by a transaction.
 * It may have a parent which is only used for read lookups.
 */
internal class STMFrame(val parent: STMFrame? = null) : STM {

  class Entry(var initialVal: Any?, var newVal: Any?) {
    object NO_CHANGE
    object NOT_PRESENT

    fun isWrite(): Boolean =
      newVal !== NO_CHANGE

    fun update(v: Any?): Unit {
      newVal = if (initialVal === v) NO_CHANGE else v
    }

    fun getValue(): Any? = if (isWrite()) newVal else initialVal
  }

  object RETRYING

  internal val accessMap = mutableMapOf<TVar<Any?>, Entry>()

  /**
   * Helper to search the entire hierarchy for stored previous reads
   */
  private fun readVar(v: TVar<Any?>): Any? =
    accessMap[v]?.getValue() ?: parent?.readVar(v) ?: Entry.NOT_PRESENT

  /**
   * Retry yields to the runloop and never resumes.
   *
   * This could be modeled with exceptions as well, but that complicates a users exceptions handling
   *  and in general does not seem as nice.
   */
  override suspend fun retry(): Nothing = suspendCoroutine {}

  override suspend fun <A> (suspend STM.() -> A).orElse(other: suspend STM.() -> A): A =
    runLocal(this@orElse, { this@STMFrame.other() }) { throw it }

  override suspend fun <A> catch(f: suspend STM.() -> A, onError: suspend STM.(Throwable) -> A): A =
    runLocal(f, { this@STMFrame.retry() }) { this@STMFrame.onError(it) }

  private inline fun <A> runLocal(
    crossinline f: suspend STM.() -> A,
    onRetry: () -> A,
    onError: (Throwable) -> A
  ): A {
    while (true) {
      val frame = STMFrame(this@STMFrame)
      try {
        f.startCoroutineUninterceptedOrReturn(frame, Continuation(EmptyCoroutineContext) {
          throw IllegalStateException("STM transaction was resumed after aborting. How?!")
        }).let {
          // Validate the inner frame right now to check for a quick early abort and a cheaper retry
          //  If we are already invalid here there is no point in continuing.
          if (frame.validate()) {
            if (it == COROUTINE_SUSPENDED) {
              this@STMFrame.mergeReads(frame)
              return@runLocal onRetry()
            } else {
              this@STMFrame.merge(frame)
              return@runLocal it as A
            }
          }
        }
      } catch (e: Throwable) {
        // An invalid frame retries even if it throws, so our sub-frame also needs to handle this correctly
        if (frame.validate()) {
          this@STMFrame.mergeReads(frame)
          return@runLocal onError(e)
        }
      }
    }
  }

  /**
   * First checks if we have already read this variable, if not it reads it and stores the result
   */
  override suspend fun <A> TVar<A>.read(): A =
    when (val r = readVar(this as TVar<Any?>)) {
      Entry.NOT_PRESENT -> unsafeRead().also { accessMap[this] = Entry(it, Entry.NO_CHANGE) }
      else -> r as A
    }

  /**
   * Add a write to the write set.
   *
   * If we have not seen this variable before we add a read which stores it in the read set as well.
   */
  override suspend fun <A> TVar<A>.write(a: A): Unit =
    accessMap[this as TVar<Any?>]?.update(a) ?: unsafeRead().let { accessMap[this] = Entry(it, a) }

  internal fun validate(): Boolean =
    accessMap.all { (tv, entry) -> tv.value === entry.initialVal }

  internal fun validateAndCommit(): Boolean {
    if (accessMap.isEmpty()) return true

    val locked = mutableListOf<Map.Entry<TVar<Any?>, Entry>>()
    val reads = mutableListOf<Map.Entry<TVar<Any?>, Entry>>()

    /**
     * Why do we not lock reads?
     * To answer this question we need to ask under what conditions a transaction may commit:
     * - A transaction can commit if all values read contain the same value when committing
     *
     * This means that when we hold all write locks we just need to verify that all our reads are consistent, any change after
     *  that has no effect on this transaction because our write will 100% persist consistently (we hold all locks) and
     *  any other transaction depending on a variable we are about to write to has to wait for us and then verify again
     */
    accessMap.forEach { tvToEntry ->
      val (tv, entry) = tvToEntry
      if (entry.isWrite()) {
        if (tv.lock_cond(this, entry.initialVal)) {
          locked.add(tvToEntry)
        } else {
          locked.forEach { it.key.release(this, it.value.initialVal) }
          return@validateAndCommit false
        }
      } else {
        if (tv.value !== entry.initialVal) {
          locked.forEach { it.key.release(this, it.value.initialVal) }
          return@validateAndCommit false
        } else {
          reads.add(tvToEntry)
        }
      }
    }

    if (reads.any { (tv, entry) -> tv.value !== entry.initialVal }) {
      locked.forEach { it.key.release(this, it.value.initialVal) }
      return false
    }

    locked.forEach { it.key.release(this, it.value.newVal) }
    // TODO Evaluate if this needs to be separate or if it is cheap enough to do above.
    //  Basically any work done before all locks are released needs to be cheap and this avoids a bit of work.
    locked.forEach { it.key.notify() }
    return true
  }

  private fun mergeReads(other: STMFrame): Unit {
    accessMap.putAll(other.accessMap.filter { (_, e) -> e.isWrite().not() })
  }

  private fun merge(other: STMFrame): Unit {
    accessMap.putAll(other.accessMap)
  }
}

/**
 * In some special cases it is possible to detect if a STM transaction blocks indefinitely so we can
 *  abort here.
 */
object BlockedIndefinitely : Throwable("Transaction blocked indefinitely")

// --------
/**
 * Wrapper for a running transaction.
 *
 * Keeps the continuation that [TVar]'s use to resume this transaction.
 */
internal class STMTransaction<A>(val f: suspend STM.() -> A) {
  private val cont = atomic<Continuation<Unit>?>(null)

  /**
   * Any one resumptions is enough, because we enqueue on all read variables this might be called multiple times.
   */
  fun getCont(): Continuation<Unit>? = cont.getAndSet(null)

  // TODO should we abort after retrying x times to help a user notice "live-locked" transactions?
  //  This could be implemented by checking two values when retrying:
  //  - the number of prior retries
  //  - the time since we started trying to commit this transaction
  //  If they both pass a threshold we should probably kill the transaction and throw
  //  "live-locked" transactions are those that are continuously retry due to accessing variables with high contention and
  //   taking longer than the transactions updating those variables.
  suspend fun commit(): A {
    loop@while (true) {
      val frame = STMFrame()
      try {
        f.startCoroutineUninterceptedOrReturn(frame, Continuation(EmptyCoroutineContext) {
          throw IllegalStateException("STM transaction was resumed after aborting. How?!")
        }).let {
          if (it != COROUTINE_SUSPENDED) {
            // try commit
            if (frame.validateAndCommit()) {
              return@commit it as A
            }
            // retry
          } else {
            // blocking retry
            if (frame.accessMap.isEmpty()) throw BlockedIndefinitely

            val registered = mutableListOf<TVar<Any?>>()
            suspendCoroutine<Unit> susp@{ k ->
              cont.value = k

              frame.accessMap
                .forEach { (tv, entry) ->
                  if (tv.registerWaiting(this, entry.initialVal)) registered.add(tv)
                  else return@susp
                }
            }
            registered.forEach { it.removeWaiting(this) }
          }
        }
      } catch (e: Throwable) {
        // An invalid transaction should be retried even if it threw an exception
        if (frame.validate()) throw e
      }
    }
  }
}
