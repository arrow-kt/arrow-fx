package arrow.fx.coroutines

import arrow.fx.coroutines.stm.TVar
import kotlinx.atomicfu.atomic
import kotlin.coroutines.Continuation
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.RestrictsSuspension
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.startCoroutineUninterceptedOrReturn
import kotlin.coroutines.suspendCoroutine

@RestrictsSuspension
interface STM {
  /**
   * Abort the current transaction.
   *
   * This semantically-blocks until any of the accessed [TVar]'s changed.
   */
  suspend fun retry(): Nothing

  /**
   * Run the given transaction and fallback to the other one if the first one calls [retry].
   *
   * TODO: For simplicity this does not validate reads it only looks for [retry] so failing reads will not
   *  trigger the fallback right here. Not sure if this is a problem...
   */
  suspend infix fun <A> (suspend STM.() -> A).orElse(other: suspend STM.() -> A): A

  /**
   * Read the value from a [TVar].
   *
   * This comes with a few guarantees:
   * - Any given [TVar] is only ever read once during a transaction.
   * - When committing the transaction the value read has to be equal to the current value otherwise the
   *   transaction will retry
   * - The above is guaranteed through any nesting of STM blocks (via [orElse] or other combinators)
   */
  suspend fun <A> TVar<A>.read(): A

  /**
   * Set the value of a [TVar].
   *
   * Similarly to [read] this comes with a few guarantees:
   * - For multiple writes to the same [TVar] in a transaction only the last will actually be performed
   * - When committing the value inside the [TVar] at the time of calling [write] has to be the
   *   same as the current value otherwise the transaction will retry
   * - The above is guaranteed through any nesting of STM blocks (via [orElse] or other combinators)
   */
  suspend fun <A> TVar<A>.write(a: A): Unit
}

/**
 * Helper to create stm blocks that can be run with [orElse]
 */
fun <A> stm(f: suspend STM.() -> A): suspend STM.() -> A = f

/**
 * Retry if [b] is false otherwise does nothing.
 *
 * `check(b) = if (b.not()) retry() else Unit`
 */
suspend fun <A> STM.check(b: Boolean): Unit = if (b.not()) retry() else Unit

/**
 * Create a new [TVar] inside a transaction, because [TVar.new] is not possible inside [STM] transactions.
 */
suspend fun <A> STM.newVar(a: A): TVar<A> = TVar(a)

/**
 * Run a transaction to completion.
 *
 * This may suspend if [retry] is called and no [TVar] changed (It resumes automatically on changes)
 *
 * This also rethrows all exceptions thrown inside [f].
 */
suspend fun <A> atomically(f: suspend STM.() -> A): A = STMTransaction(f).commit()

// ------------
/**
 * A STMFrame keeps the reads and writes performed by a transaction.
 * It may have a parent which is only used for read lookups.
 */
internal class STMFrame(val parent: STMFrame? = null) : STM {
  private val readSet = mutableMapOf<TVar<*>, Any?>()
  private val writeSet = mutableMapOf<TVar<*>, Any?>()

  /**
   * Helper to search the entire hierarchy for stored previous reads
   */
  private fun readVar(v: TVar<*>): Any? = readSet[v] ?: parent?.readVar(v)

  /**
   * Helper to check if a [TVar] has already been read inside the transaction
   */
  private fun hasVar(v: TVar<*>): Boolean = readSet.containsKey(v) || parent?.hasVar(v) == true

  /**
   * Retry yields to the runloop and never resumes.
   *
   * This could be modeled with exceptions as well, but that complicates a users exceptions handling
   *  and in general does not seem as nice.
   */
  override suspend fun retry(): Nothing = suspendCoroutine {}

  override suspend fun <A> (suspend STM.() -> A).orElse(other: suspend STM.() -> A): A =
    partialTransaction(this@STMFrame, this@orElse)
      ?: partialTransaction(this@STMFrame, other)
      ?: retry()

  /**
   * First checks if we have already read this variable, if not it reads it and stores the result
   */
  override suspend fun <A> TVar<A>.read(): A {
    val v = readVar(this) ?: unsafeRead().also { readSet[this] = it }
    return v as A
  }

  /**
   * Add a write to the write set.
   *
   * If we have not seen this variable before we add a read which stores it in the read set as well.
   */
  override suspend fun <A> TVar<A>.write(a: A) {
    if (hasVar(this).not()) read()
    writeSet[this] = a
  }

  /**
   * Utility which locks all read [TVar]'s and passes them to [f].
   *
   * The second argument is the release function which releases all locks, but does not
   *  notify waiters via [TVar.notify].
   */
  internal inline fun <A> withLockedReadSet(
    f: (List<Triple<TVar<Any?>, Any?, Any?>>, () -> Unit) -> A
  ): A {
    val readsOrdered = readSet.toList().sortedBy { it.first.id }
      .map { (tv, v) -> Triple(tv as TVar<Any?>, tv.lock(this), v) }
    return f(readsOrdered) { readsOrdered.forEach { (tv, nV, _) -> (tv).release(this, nV) } }
  }

  /**
   * Helper which automatically releases after [f] is done.
   */
  private inline fun <A> withLockedReadSetAndRelease(f: (List<Triple<TVar<Any?>, Any?, Any?>>) -> A): A =
    withLockedReadSet { xs, rel -> f(xs).also { rel() } }

  /**
   * Validate and commit changes from this frame.
   *
   * Returns whether or not validation (and thus the commit) was successful.
   */
  fun validateAndCommit(): Boolean = withLockedReadSetAndRelease {
    if (it.invalid()) false
    else {
      it.forEach { (tv, _, _) ->
        writeSet[tv]?.let { tv.release(this, it).also { tv.notify() } }
      }.let { true }
    }
  }

  fun mergeChildReads(c: STMFrame): Unit {
    readSet.putAll(c.readSet)
  }

  fun mergeChildWrites(c: STMFrame): Unit {
    writeSet.putAll(c.writeSet)
  }
}

/**
 * Helper which checks whether or not any variables changed.
 *
 * The second variable from the triple is the current value
 *  and the third is the one we saw back when calling read.
 */
internal fun List<Triple<TVar<Any?>, Any?, Any?>>.invalid(): Boolean =
  any { (_, nV, v) -> nV !== v }

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

  suspend fun commit(): A {
    loop@while (true) {
      val frame = STMFrame()
      f.startCoroutineUninterceptedOrReturn(frame, Continuation(EmptyCoroutineContext) {
        throw IllegalStateException("STM transaction was resumed after aborting. How?!")
      }).let {
        if (it == COROUTINE_SUSPENDED) {
          // blocking retry
          frame.withLockedReadSet { xs, releaseVars ->
            // quick sanity check. If a transaction has not read anything and retries we are blocked forever
            if (xs.isEmpty()) throw BlockedIndefinitely

            // check if a variable changed, no need to block if that is the case
            if (xs.invalid()) releaseVars()
            else {
              // nothing changed so we need to suspend here
              xs.forEach { (tv, _, _) -> tv.queue(this) }
              // it is important to set the continuation before releasing the locks, otherwise
              //  we may not see changes
              suspendCoroutine<Unit> { k -> cont.value = k; releaseVars() }

              xs.forEach { (tv, _, _) -> tv.removeQueued(this) }
            }
          }
        } else {
          // try commit
          if (frame.validateAndCommit().not()) {
            // retry
          } else {
            return@commit it as A
          }
        }
      }
    }
  }
}

/**
 * Partially run a transaction.
 *
 * This does not do a fully validation, it only checks if we suspend (retry was called).
 */
private fun <A> partialTransaction(parent: STMFrame, f: suspend STM.() -> A): A? {
  val frame = STMFrame(parent)
  return f.startCoroutineUninterceptedOrReturn(frame, Continuation(EmptyCoroutineContext) {
    throw IllegalStateException("STM transaction was resumed after aborting. How?!")
  }).let {
    if (it == COROUTINE_SUSPENDED) {
      parent.mergeChildReads(frame)
      null
    } else {
      parent.mergeChildReads(frame)
      parent.mergeChildWrites(frame)
      it as A
    }
  }
}

/**
 * A few things to consider:
 * We can remove the need for merging nested reads by always saving them in parents because we end
 *  up merging them anyways...
 * This implements fine-grained locking where we lock each TVar accessed. STM can also be implemented
 *  with more coarse locks, although I kind of think that is a waste as it usually leads to linearity
 *  even for unrelated transactions...
 * Locking right now actually blocks a thread by means of looping over variables. This is usually
 *  fine because the only time those variables are locked is when a transaction commits
 *  which is assumed to be infrequent. For high congestion variables using traditional locks is better.
 *  We could also do this over suspend by suspending until the lock is released but I don't think that's
 *  beneficial at all.
 * GHC sometimes kills transactions pre-emptively when it knows it will fail, ignoring why it can do that I don't
 *  see a good way to implement this here because we never yield in the run-loop (except when aborting).
 *  Is there a good way to interrupt a running transaction?
 * The issue with transactions leaking because the are saved to TVars can be solved in a few different ways,
 *  for which the easiest may be removing a transaction from all variables in the read set when
 *  the transaction either aborts or finishes.
 * OrElse currently only checks for calls to retry and does not validate reads/writes. I have no idea
 *  if this is consistent with ghc because the docs are a bit unclear in that regard.
 *  This right now is the easiest implementation though and reads and writes are validated at the end
 *  anyway. But the inner Frame is gone by that time and orElse is not running.
 *  I can probably get this to work in a way which validates the transaction and keeps this in mind but
 *  that will make the implementation a bit more complex.
 */
