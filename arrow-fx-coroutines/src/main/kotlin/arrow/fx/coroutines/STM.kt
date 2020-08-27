package arrow.fx.coroutines

import arrow.core.Tuple2
import arrow.core.toT
import arrow.fx.coroutines.stm.TArray
import arrow.fx.coroutines.stm.TMVar
import arrow.fx.coroutines.stm.TMap
import arrow.fx.coroutines.stm.TQueue
import arrow.fx.coroutines.stm.TSem
import arrow.fx.coroutines.stm.TSet
import arrow.fx.coroutines.stm.TVar
import kotlinx.atomicfu.atomic
import kotlin.coroutines.Continuation
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.RestrictsSuspension
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.startCoroutineUninterceptedOrReturn
import kotlin.coroutines.suspendCoroutine

/**
 * Software transactional memory, or STM, is an abstraction for concurrent state modification.
 * With [STM] one can write concurrent abstractions that can easily be composed without
 *  exposing details of how it ensures safety guarantees.
 * Programs written with [STM] will neither deadlock nor have race-conditions.
 *
 * Such guarantees are usually not possible with other forms of concurrent communication such as locks,
 *  atomic variables or [ConcurrentVar].
 *
 * > The api of [STM] is based on the haskell package [stm](https://hackage.haskell.org/package/stm) and
 *   the implementation is quite different to ensure the same semantics within kotlin.
 *
 * -- Examples and docs here --
 *
 * Further reading:
 * - [Composable memory transactions, by Tim Harris, Simon Marlow, Simon Peyton Jones, and Maurice Herlihy, in ACM Conference on Principles and Practice of Parallel Programming 2005.](https://www.microsoft.com/en-us/research/publication/composable-memory-transactions/)
 */
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

  /**
   * Modify the value of a [TVar]
   *
   * `modify(f) = write(f(read()))`
   */
  suspend fun <A> TVar<A>.modify(f: (A) -> A): Unit = write(f(read()))

  /**
   * Swap the content of the [TVar]
   *
   * @return The previous value stored inside the [TVar]
   */
  suspend fun <A> TVar<A>.swap(a: A): A = read().also { write(a) }

  // -------- TMVar
  suspend fun <A> TMVar<A>.take(): A =
    v.read().also { v.write(null) } ?: retry()

  suspend fun <A> TMVar<A>.put(a: A): Unit =
    v.read()?.let { retry() } ?: v.write(a)

  suspend fun <A> TMVar<A>.read(a: A): A =
    v.read() ?: retry()

  suspend fun <A> TMVar<A>.tryTake(): A? =
    v.read()?.also { v.write(null) }

  suspend fun <A> TMVar<A>.tryPut(a: A): Boolean =
    v.read()?.let { false } ?: v.write(a).let { true }

  suspend fun <A> TMVar<A>.tryRead(): A? =
    v.read()

  suspend fun <A> TMVar<A>.isEmpty(): Boolean =
    v.read()?.let { false } ?: true

  suspend fun <A> TMVar<A>.isNotEmpty(): Boolean =
    isEmpty().not()

  suspend fun <A> TMVar<A>.swap(a: A): A =
    v.read()?.also { v.write(a) } ?: retry()

  // -------- TSemaphore
  suspend fun TSem.wait(): Unit {
    val curr = v.read()
    check(curr > 0)
    v.write(curr - 1)
  }

  suspend fun TSem.signal(): Unit = v.write(v.read() + 1)
  suspend fun TSem.signal(n: Int): Unit = when (n) {
    0 -> Unit
    1 -> signal()
    else ->
      if (n < 0) throw IllegalStateException("Cannot decrease permits using signal(n)")
      else v.write(v.read() + n)
  }

  // TQueue
  suspend fun <A> TQueue<A>.write(a: A): Unit =
    writes.modify { it + a }

  suspend fun <A> TQueue<A>.read(): A {
    val xs = reads.read()
    return if (xs.isNotEmpty()) reads.write(xs.drop(1)).let { xs.first() }
    else {
      val ys = writes.read()
      if (ys.isEmpty()) retry()
      else {
        writes.write(emptyList())
        reads.write(ys.drop(1))
        ys.first()
      }
    }
  }

  suspend fun <A> TQueue<A>.tryRead(): A? = stm { read() } orElse { null }
  suspend fun <A> TQueue<A>.flush(): List<A> {
    val xs = reads.read().also { if (it.isNotEmpty()) reads.write(emptyList()) }
    val ys = writes.read().also { if (it.isNotEmpty()) writes.write(emptyList()) }
    return xs + ys
  }

  suspend fun <A> TQueue<A>.peek(): A =
    read().also { writeFront(it) }

  suspend fun <A> TQueue<A>.tryPeek(): A? =
    tryRead()?.also { writeFront(it) }

  suspend fun <A> TQueue<A>.writeFront(a: A): Unit =
    reads.read().let { reads.write(listOf(a) + it) }

  // -------- TArray
  suspend fun <A> TArray<A>.get(i: Int): A =
    v[i].read()

  suspend fun <A> TArray<A>.write(i: Int, a: A): Unit =
    v[i].write(a)

  suspend fun <A> TArray<A>.transform(f: (A) -> A): Unit =
    v.forEach { it.modify(f) }

  suspend fun <A, B> TArray<A>.fold(init: B, f: (B, A) -> B): B =
    v.fold(init) { acc, v -> f(acc, v.read()) }

  // -------- TMap
  suspend fun <K, V> TMap<K, V>.get(k: K): V =
    v.read()[k]?.read() ?: retry()

  suspend fun <K, V> TMap<K, V>.getOrNull(k: K): V? =
    v.read()[k]?.read()

  suspend fun <K, V> TMap<K, V>.insert(k: K, va: V): Unit =
    alter(k) { va }

  suspend fun <K, V> TMap<K, V>.alter(k: K, f: (V?) -> V?): Unit {
    val m = v.read()
    if (m.containsKey(k)) {
      val tv = m.getValue(k)
      val nV = f(tv.read())
      if (nV == null) v.write(m.toMutableMap().also { it.remove(k) })
      else tv.write(nV)
    } else {
      val nV = f(null)
      if (nV != null) newTVar(nV).let { v.write(m + (k to it)) }
    }
  }

  suspend fun <K, V> TMap<K, V>.remove(k: K): Unit =
    alter(k) { null }

  suspend fun <K, V> TMap<K, V>.update(k: K, f: (V) -> V): Unit =
    alter(k) { it?.let(f) }

  suspend fun <K, V> TMap<K, V>.member(k: K): Boolean =
    v.read().containsKey(k)

  suspend fun <K, V> TMap<K, V>.toList(): List<Tuple2<K, V>> =
    v.read().toList().map { (k, tv) -> k toT tv.read() }

  suspend fun <K, V> TMap<K, V>.size(): Int =
    v.read().size

  // -------- TSet
  suspend fun <A> TSet<A>.insert(a: A): Unit =
    newTVar(a).let { v.read() + it }

  suspend fun <A> TSet<A>.remove(a: A): Unit =
    v.write(v.read().filter { it.read() != a }.toSet())

  suspend fun <A> TSet<A>.size(): Int =
    v.read().size

  suspend fun <A> TSet<A>.member(a: A): Boolean =
    v.read().any { it.read() == a }

  suspend fun <A> TSet<A>.toList(): List<A> =
    v.read().map { it.read() }
}

/**
 * Helper to create stm blocks that can be run with [orElse]
 */
inline fun <A> stm(noinline f: suspend STM.() -> A): suspend STM.() -> A = f

/**
 * Retry if [b] is false otherwise does nothing.
 *
 * `check(b) = if (b.not()) retry() else Unit`
 */
suspend fun STM.check(b: Boolean): Unit = if (b.not()) retry() else Unit

/**
 * Create a new [TVar] inside a transaction, because [TVar.new] is not possible inside [STM] transactions.
 */
suspend fun <A> STM.newTVar(a: A): TVar<A> = TVar(a)

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
  private fun readVar(v: TVar<*>): Any? = writeSet[v] ?: parent?.readVar(v) ?: readSet[v]

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
   * Utility which locks all read [TVar]'s and passes them to [withLocked].
   *
   * The second argument is the release function which releases all locks, but does not
   *  notify waiters via [TVar.notify].
   *
   * The second argument to [withValidAndLockedReadSet] is a fallback to execute when
   *  a [TVar] was invalid. This releases all locks and returns the result of [onInvalid].
   */
  internal inline fun <A> withValidAndLockedReadSet(
    withLocked: (List<Triple<TVar<Any?>, Any?, Any?>>, () -> Unit) -> A,
    onInvalid: () -> A
  ): A {
    val readsOrdered: MutableList<Triple<TVar<Any?>, Any?, Any?>> = mutableListOf()
    val release: () -> Unit = { readsOrdered.forEach { (tv, nV, _) -> (tv).release(this, nV) } }

    readSet.toList().sortedBy { it.first.id }
      .forEach { (tv, v) ->
        val res = tv.lock(this)
        readsOrdered.add(Triple(tv as TVar<Any?>, res, v))
        if (res !== v) {
          release()
          return@withValidAndLockedReadSet onInvalid()
        }
      }
    return withLocked(readsOrdered, release)
  }

  /**
   * Helper which automatically releases after [withLocked] is done.
   */
  private inline fun <A> withValidAndLockedReadSetAndRelease(
    withLocked: (List<Triple<TVar<Any?>, Any?, Any?>>) -> A,
    onInvalid: () -> A
  ): A =
    withValidAndLockedReadSet({ xs, rel -> withLocked(xs).also { rel() } }, onInvalid)

  /**
   * Validate and commit changes from this frame.
   *
   * Returns whether or not validation (and thus the commit) was successful.
   */
  fun validateAndCommit(): Boolean = withValidAndLockedReadSetAndRelease({
    it.forEach { (tv, _, _) ->
      writeSet[tv]?.let { tv.release(this, it).also { tv.notify() } }
    }
    true
  }) { false }

  fun mergeChildReads(c: STMFrame): Unit {
    readSet.putAll(c.readSet)
  }

  fun mergeChildWrites(c: STMFrame): Unit {
    writeSet.putAll(c.writeSet)
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

  // TODO Check if validating twice once without locking and once with is worthwhile
  //  Fine grain locks are all about optimistic running which means they are fastest if there
  //   are no conflicts thus this may not be great for that case.
  //  Checking before taking locks sounds like something global lock implementations benefit
  //   a lot more than fine grained locking ones
  //  For reference the current implementation fails fast during lock acquisition as soon
  //   as it sees an invalid value and then releases all already acquired locks.
  suspend fun commit(): A {
    loop@ while (true) {
      val frame = STMFrame()
      f.startCoroutineUninterceptedOrReturn(frame, Continuation(EmptyCoroutineContext) {
        throw IllegalStateException("STM transaction was resumed after aborting. How?!")
      }).let {
        if (it == COROUTINE_SUSPENDED) {
          // blocking retry
          frame.withValidAndLockedReadSet({ xs, releaseVars ->
            // quick sanity check. If a transaction has not read anything and retries we are blocked forever
            if (xs.isEmpty()) throw BlockedIndefinitely

            // nothing changed so we need to suspend here
            xs.forEach { (tv, _, _) -> tv.queue(this) }
            // it is important to set the continuation before releasing the locks, otherwise
            //  we may not see changes
            suspendCoroutine<Unit> { k -> cont.value = k; releaseVars() }

            xs.forEach { (tv, _, _) -> tv.removeQueued(this) }
          }, {})
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
