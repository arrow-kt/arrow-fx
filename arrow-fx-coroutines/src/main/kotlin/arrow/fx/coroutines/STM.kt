package arrow.fx.coroutines

import arrow.fx.coroutines.stm.TArray
import arrow.fx.coroutines.stm.TMVar
import arrow.fx.coroutines.stm.TQueue
import arrow.fx.coroutines.stm.TSem
import arrow.fx.coroutines.stm.TVar
import kotlinx.atomicfu.atomic
import kotlin.coroutines.Continuation
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.RestrictsSuspension
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.startCoroutineUninterceptedOrReturn
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

/**
 * # Consistent and safe concurrent state updates
 *
 * Software transactional memory, or STM, is an abstraction for concurrent state modification.
 * With [STM] one can write concurrent abstractions that can easily be composed without
 *  exposing details of how it ensures safety guarantees.
 * Programs written with [STM] will neither deadlock nor have race-conditions.
 *
 * Such guarantees are usually not possible with other forms of concurrent communication such as locks,
 *  atomic variables or [ConcurrentVar].
 *
 * > The api of [STM] is based on the haskell package [stm](https://hackage.haskell.org/package/stm).
 *
 * The base building blocks of [STM] are [TVar]'s and a few primitives [retry], [orElse] and [catch].
 *
 * ## STM Datastructures
 *
 * There are several datastructures built on top of [TVar]'s already provided out of the box:
 * - [TQueue]: A transactional mutable queue
 * - [TMVar]: A mutable transactional variable that may be empty
 * - [TArray]: Array of [TVar]'s
 * - [TSem]: Transactional semaphore
 * - [TVar]: A transactional mutable variable
 *
 * All of these structures (excluding [TVar]) are built upon [TVar]'s and the [STM] primitives and implementing other
 *  datastructures with [STM] can be done by composing the existing structures.
 *
 * ## Reading and writing to concurrent state:
 *
 * In order to modify transactional datastructures we have to be inside the [STM] context. This is achieved either by defining our
 *  functions with [STM] as the receiver or using [stm] to define functions.
 *
 * Running a transaction is then done using [atomically].
 *
 * > Note: A transaction that sees an invalid state (a [TVar] that was read has been changed concurrently) it will restart and try again.
 *   This essentially means we start from scratch, therefore it is recommended to keep transactions small and to never use code that
 *   has side-effects inside. We use `@RestrictSuspension` to disallow the use of suspension functions but functions that are not suspended
 *   and execute side-effects can be called. This is bad practice however for multiple reasons:
 *   - Transactions may be aborted at any time so accessing resources may never trigger finalizers
 *   - Transactions may rerun an arbitrary amount of times before finishing and thus all effects will rerun.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.coroutines.Environment
 * import arrow.fx.coroutines.atomically
 * import arrow.fx.coroutines.stm.TVar
 * import arrow.fx.coroutines.STM
 *
 * //sampleStart
 * suspend fun STM.transfer(from: TVar<Int>, to: TVar<Int>, amount: Int): Unit {
 *   withdraw(from, amount)
 *   deposit(to, amount)
 * }
 *
 * suspend fun STM.deposit(acc: TVar<Int>, amount: Int): Unit {
 *   val current = acc.read()
 *   acc.write(current + amount)
 *   // or the shorthand acc.modify { it + amount }
 * }
 *
 * suspend fun STM.withdraw(acc: TVar<Int>, amount: Int): Unit {
 *   val current = acc.read()
 *   if (current - amount >= 0) acc.write(current + amount)
 *   else throw IllegalStateException("Not enough money in the account!")
 * }
 * //sampleEnd
 *
 * fun main() {
 *   Environment().unsafeRunSync {
 *     val acc1 = TVar.new(500)
 *     val acc2 = TVar.new(300)
 *     println("Balance account 1: ${acc1.unsafeRead()}")
 *     println("Balance account 2: ${acc2.unsafeRead()}")
 *     println("Performing transaction")
 *     atomically { transfer(acc1, acc2, 50) }
 *     println("Balance account 1: ${acc1.unsafeRead()}")
 *     println("Balance account 2: ${acc2.unsafeRead()}")
 *   }
 * }
 * ```
 * This example shows a banking service moving money from one account to the other with [STM].
 * Should the first account not have enough money we throw an exception. This code is guaranteed to never deadlock and to never
 *  produce an invalid state by out of order updates. These guarantees follow from the semantics of [STM] itself and are universal
 *  to all [STM] programs.
 *
 * ## Retrying manually
 *
 * It is sometimes beneficial to manually abort a transaction until a variable changes. This can be for a variety of reasons such as
 *  seeing an invalid state or having no value to read.
 *
 * Inside a transaction we can always call [retry] to trigger an immediate abort. The transaction will suspend and be resumed as soon
 *  as one of the variables that has been accessed by this transaction changes.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.coroutines.Environment
 * import arrow.fx.coroutines.atomically
 * import arrow.fx.coroutines.stm.TVar
 * import arrow.fx.coroutines.ForkConnected
 * import arrow.fx.coroutines.seconds
 * import arrow.fx.coroutines.sleep
 * import arrow.fx.coroutines.STM
 *
 * //sampleStart
 * suspend fun STM.transfer(from: TVar<Int>, to: TVar<Int>, amount: Int): Unit {
 *   withdraw(from, amount)
 *   deposit(to, amount)
 * }
 *
 * suspend fun STM.deposit(acc: TVar<Int>, amount: Int): Unit {
 *   val current = acc.read()
 *   acc.write(current + amount)
 *   // or the shorthand acc.modify { it + amount }
 * }
 *
 * suspend fun STM.withdraw(acc: TVar<Int>, amount: Int): Unit {
 *   val current = acc.read()
 *   if (current - amount >= 0) acc.write(current + amount)
 *   else retry() // we now retry if there is not enough money in the account
 *   // this can also be achieved by using `check(current - amount >= 0); acc.write(it + amount)`
 * }
 * //sampleEnd
 *
 * fun main() {
 *   Environment().unsafeRunSync {
 *     val acc1 = TVar.new(0)
 *     val acc2 = TVar.new(300)
 *     println("Balance account 1: ${acc1.unsafeRead()}")
 *     println("Balance account 2: ${acc2.unsafeRead()}")
 *     ForkConnected {
 *       println("Sending money - Searching")
 *       sleep(2.seconds)
 *       println("Sending money - Found some")
 *       atomically { acc1.write(100_000_000) }
 *     }
 *     println("Performing transaction")
 *     atomically {
 *       println("Trying to transfer")
 *       transfer(acc1, acc2, 50)
 *     }
 *     println("Balance account 1: ${acc1.unsafeRead()}")
 *     println("Balance account 2: ${acc2.unsafeRead()}")
 *   }
 * }
 * ```
 *
 * Here in this (silly) example we changed `withdraw` to retry and thus wait until enough money is in the account, which after
 *  a few seconds just happens to be the case.
 *
 * [retry] can be used to implement a lot of complex transactions and many datastructures like [TMVar] or [TQueue] use to to great effect.
 *
 * > Note: [retry] will suspend a transaction until a variable updates. It will not block, but if no variable is updated it will wait forever!
 *
 * ## Branching with [orElse]
 *
 * [orElse] is another important primitive which allows a user to detect if a branch called [retry] and then use a fallback instead.
 * If both branches [retry] the entire transaction will [retry].
 *
 * ## Exceptions
 *
 * Throwing inside [STM] will let the exception bubble up to either a [catch] handler or to [atomically] which will rethrow it.
 *
 * > Note: Using `try {...} catch (e: Exception) {...}` is not encouraged because any state change inside `try` will not be undone when
 *   an exception occurs! The recommended way of catching exceptions is to use [catch] which properly discards those changes.
 *
 * Further reading:
 * - [Composable memory transactions, by Tim Harris, Simon Marlow, Simon Peyton Jones, and Maurice Herlihy, in ACM Conference on Principles and Practice of Parallel Programming 2005.](https://www.microsoft.com/en-us/research/publication/composable-memory-transactions/)
 */
// TODO Explore this https://dl.acm.org/doi/pdf/10.1145/2976002.2976020 when benchmarks are set up
@RestrictsSuspension
interface STM {
  /**
   * Rerun the current transaction.
   *
   * This semantically-blocks until any of the accessed [TVar]'s changed.
   */
  suspend fun retry(): Nothing

  /**
   * Run the given transaction and fallback to the other one if the first one calls [retry].
   */
  suspend infix fun <A> (suspend STM.() -> A).orElse(other: suspend STM.() -> A): A

  suspend fun <A> catch(f: suspend STM.() -> A, onError: suspend STM.(Throwable) -> A): A

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

  /**
   * Create a new [TVar] inside a transaction, because [TVar.new] is not possible inside [STM] transactions.
   */
  suspend fun <A> newTVar(a: A): TVar<A> = TVar(a)

  // -------- TMVar
  suspend fun <A> TMVar<A>.take(): A =
    v.read().also { v.write(null) } ?: retry()

  suspend fun <A> TMVar<A>.put(a: A): Unit =
    v.read()?.let { retry() } ?: v.write(a)

  suspend fun <A> TMVar<A>.read(): A =
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
  suspend fun TSem.available(): Int =
    v.read()

  suspend fun TSem.acquire(): Unit =
    acquire(1)

  suspend fun TSem.acquire(n: Int): Unit {
    val curr = v.read()
    check(curr - n >= 0)
    v.write(curr - n)
  }

  suspend fun TSem.tryAcquire(): Boolean =
    tryAcquire(1)

  suspend fun TSem.tryAcquire(n: Int): Boolean =
    stm { acquire(n); true } orElse { false }

  suspend fun TSem.release(): Unit = v.write(v.read() + 1)
  suspend fun TSem.release(n: Int): Unit = when (n) {
    0 -> Unit
    1 -> release()
    else ->
      if (n < 0) throw IllegalArgumentException("Cannot decrease permits using signal(n). n was negative: $n")
      else v.write(v.read() + n)
  }

  suspend fun <A> TSem.withPermit(f: suspend STM.() -> A): A {
    acquire()
    return f().also { release() }
  }

  suspend fun <A> TSem.withPermit(n: Int, f: suspend STM.() -> A): A {
    acquire(n)
    return f().also { release(n) }
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

  suspend fun <A> TQueue<A>.tryRead(): A? =
    (stm { read() } orElse { null })

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

  suspend fun <A> TQueue<A>.isEmpty(): Boolean =
    reads.read().isEmpty() && writes.read().isEmpty()

  suspend fun <A> TQueue<A>.isNotEmpty(): Boolean =
    reads.read().isNotEmpty() || writes.read().isNotEmpty()

  suspend fun <A> TQueue<A>.filter(pred: (A) -> Boolean): Unit {
    reads.modify { it.filter(pred) }
    writes.modify { it.filter(pred) }
  }

  suspend fun <A> TQueue<A>.filterNot(pred: (A) -> Boolean): Unit {
    reads.modify { it.filterNot(pred) }
    writes.modify { it.filterNot(pred) }
  }

  suspend fun <A> TQueue<A>.size(): Int = reads.read().size + writes.read().size

  // -------- TArray
  suspend fun <A> TArray<A>.get(i: Int): A =
    v[i].read()

  suspend fun <A> TArray<A>.write(i: Int, a: A): Unit =
    v[i].write(a)

  suspend fun <A> TArray<A>.transform(f: (A) -> A): Unit =
    v.forEach { it.modify(f) }

  suspend fun <A, B> TArray<A>.fold(init: B, f: (B, A) -> B): B =
    v.fold(init) { acc, v -> f(acc, v.read()) }
}

/**
 * Helper to create stm blocks that can be run with [orElse]
 *
 * Equal to [suspend] just with an [STM] receiver.
 */
inline fun <A> stm(noinline f: suspend STM.() -> A): suspend STM.() -> A = f

/**
 * Retry if [b] is false otherwise does nothing.
 *
 * `check(b) = if (b.not()) retry() else Unit`
 */
suspend fun STM.check(b: Boolean): Unit = if (b.not()) retry() else Unit

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
    when (val r = partialTransaction(this@STMFrame, this@orElse)) {
      RETRYING -> other()
      else -> r as A
    }

  override suspend fun <A> catch(f: suspend STM.() -> A, onError: suspend STM.(Throwable) -> A): A =
    try {
      // This is important to keep changes local.
      // With just try catch we'd keep state changes from try even if we throw!
      when (val r = partialTransaction(this@STMFrame, f)) {
        RETRYING -> retry()
        else -> r as A
      }
    } catch (e: Throwable) {
      onError(e)
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

  /**
   * Utility which locks all [TVar]'s that were written to and passes them to [withLocked].
   *
   * The second argument to [withLocked] is the release function which releases all taken locks, but does not
   *  notify waiters via [TVar.notify].
   *
   * The second argument to [withValidAndLockedReadSet] is a fallback to execute when
   *  a [TVar] was invalid. This releases all locks and returns the result of [onInvalid].
   */
  internal inline fun <A> withValidAndLockedReadSet(
    withLocked: (List<Pair<TVar<Any?>, Entry>>, () -> Unit) -> A,
    onInvalid: () -> A
  ): A {
    val writesOrdered: MutableList<Pair<TVar<Any?>, Entry>> = mutableListOf()
    val release: () -> Unit = { writesOrdered.forEach { (tv, entry) -> (tv).release(this, entry.initialVal) } }

    /**
     * Why do we not lock reads?
     * To answer this question we need to ask under what conditions a transaction may commit:
     * - A transaction can commit if all values read contain the same value when committing
     *
     * This means that when we hold all write locks we just need to verify that all our reads are consistent, any change after
     *  that has no effect on this transaction because our write will 100% persist consistently (we hold all locks) and
     *  any other transaction depending on a variable we are about to write to has to wait for us and then verify again
     */
    // TODO I would not need a lock order if I retry when encountering a locked TVar which essentially removes the need for ids and sorting
    //  at the cost of two potentially deadlocking transactions now having two retry rather than wait
    //  I am pretty sure that is an optimization, but I'd love benchmarks for this.
    accessMap.toList()
      // TODO this check could be done while acquiring write locks, right now this checks writes twice, once here and once during
      //  locking. I'll wait for benchmarks on this one.
      .also {
        // quick check if any values are already invalid
        // This is not strictly necessary but helps because it avoids taking locks
        if (it.any { (tv, entry) -> tv.readI() !== entry.initialVal })
          return@withValidAndLockedReadSet onInvalid()
      } // acquire locks for writes and recheck if the writes are valid
      .partition { it.second.isWrite() }
      .let { (writes, reads) ->
        // acquire locks for writes and short circuit if those became invalid
        writes.sortedBy { it.first.id }
          .forEach { (tv, entry) ->
            val curr = tv.lock(this)
            if (curr !== entry.initialVal) {
              tv.release(this, curr)
              release()
              return@withValidAndLockedReadSet onInvalid()
            } else {
              writesOrdered.add(tv to entry)
            }
          }
        // recheck that none of our reads became invalid
        if (reads.any { (tv, entry) -> tv.readI() !== entry.initialVal }) {
          release()
          return@withValidAndLockedReadSet onInvalid()
        }
      }
    return withLocked(writesOrdered, release)
  }

  /**
   * Helper which automatically releases after [withLocked] is done.
   */
  private inline fun <A> withValidAndLockedReadSetAndRelease(
    withLocked: (List<Pair<TVar<Any?>, Entry>>) -> A,
    onInvalid: () -> A
  ): A =
    withValidAndLockedReadSet({ xs, rel -> withLocked(xs).also { rel() } }, onInvalid)

  /**
   * Validate and commit changes from this frame.
   *
   * Returns whether or not validation (and thus the commit) was successful.
   */
  fun validateAndCommit(): Boolean = withValidAndLockedReadSetAndRelease({
    it.forEach { (tv, entry) ->
      tv.release(this, entry.newVal)
    }
    true
  }) { false }

  fun notifyChanges(): Unit =
    accessMap.asSequence()
      .filter { (_, e) -> e.isWrite() }
      .forEach { (tv, _) -> tv.notify() }

  fun mergeReads(other: STMFrame): Unit {
    accessMap.putAll(other.accessMap.filter { (_, e) -> e.isWrite().not() })
  }

  fun merge(other: STMFrame): Unit {
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

  suspend fun commit(): A {
    loop@ while (true) {
      val frame = STMFrame()
      f.startCoroutineUninterceptedOrReturn(frame, Continuation(EmptyCoroutineContext) {
        throw IllegalStateException("STM transaction was resumed after aborting. How?!")
      }).let {
        if (it == COROUTINE_SUSPENDED) {
          // blocking retry
          if (frame.accessMap.isEmpty()) throw BlockedIndefinitely

          val registered = mutableListOf<TVar<Any?>>()
          suspendCoroutine<Unit> susp@{ k ->
            cont.value = k

            // TODO Re-evaluate if we need to take locks here
            //  The problem is that if we don't lock other threads may update us and we might miss it and thus block
            //   despite having seen change which is really bad for one off transactions
            //   that we are polling for with stm like registerDelay and similar
            frame.accessMap.toList().sortedBy { (tv, _) -> tv.id }
              .forEach { (tv, entry) ->
                val curr = tv.lock(frame)
                if (curr !== entry.initialVal) {
                  tv.release(frame, curr)
                  cont.getAndSet(null)?.resume(Unit)
                  return@susp
                }
                tv.queue(this)
                registered.add(tv)
                tv.release(frame, curr)
              }
          }
          registered.forEach { it.removeQueued(this) }
        } else {
          // try commit
          if (frame.validateAndCommit().not()) {
            // retry
          } else {
            frame.notifyChanges()
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
 *
 */
// TODO it might make sense to validate here without locking and merge/execute each branch only if it is valid atm
//  this allows yet another concurrent early exit and a cheap retry!
private fun <A> partialTransaction(parent: STMFrame, f: suspend STM.() -> A): Any {
  val frame = STMFrame(parent)
  return f.startCoroutineUninterceptedOrReturn(frame, Continuation(EmptyCoroutineContext) {
    throw IllegalStateException("STM transaction was resumed after aborting. How?!")
  }).let {
    if (it == COROUTINE_SUSPENDED) {
      parent.mergeReads(frame)
      STMFrame.RETRYING
    } else {
      parent.merge(frame)
      it as Any
    }
  }
}
