package arrow.fx

import arrow.Kind
import arrow.Kind2
import arrow.core.Option
import arrow.fx.Queue.BackpressureStrategy
import arrow.fx.Queue.Companion.ensureCapacity
import arrow.fx.internal.ConcurrentQueue
import arrow.fx.typeclasses.Concurrent

class ForQueue private constructor() {
  companion object
}

typealias QueueOf<F, A> = Kind2<ForQueue, F, A>
typealias QueuePartialOf<F> = Kind<ForQueue, F>

@Suppress("UNCHECKED_CAST", "NOTHING_TO_INLINE")
inline fun <F, A> QueueOf<F, A>.fix(): Queue<F, A> =
  this as Queue<F, A>

/** A polymorphic effect typeclass that allows [Dequeue]'ing values from a [Queue]. */
interface Dequeue<F, A> {

  /**
   * Take a value from the [Queue], or semantically blocks until a value becomes available.
   *
   * @see [peek] for a function that doesn't remove the value from the [Queue].
   * @see [tryTake] for a function that does not semantically block but returns immediately with an [Option].
   */
  fun take(): Kind<F, A>

  /**
   * Tries to take a value from the [Queue]. Returns immediately with either [None] or a value [Some].
   *
   * @see [take] for function that semantically blocks until a value becomes available.
   * @see [tryPeek] for a function that semantically blocks until a value becomes available.
   */
  fun tryTake(): Kind<F, Option<A>>

  /**
   * Peeks a value from the [Queue] or semantically blocks until one becomes available.
   * In contrast to [take], [peek] does not remove the value from the [Queue].
   *
   * @see [tryPeek] for a function that does not semantically blocks but returns immediately with an [Option].
   */
  fun peek(): Kind<F, A>

  /**
   * Tries to peek a value from the [Queue]. Returns immediately with either [None] or a value [Some].
   * In contrast to [tryTake], [tryPeek] does not remove the value from the [Queue].
   *
   * @see [peek] for a function that semantically blocks until a value becomes available.
   */
  fun tryPeek(): Kind<F, Option<A>>

  /**
   * Takes all values currently available in the [Queue] and empties the [Queue] or returns an empty list.
   *
   * It also takes all outstanding offers, in case of a blocking strategy such as bounded.
   */
  fun takeAll(): Kind<F, List<A>>

  /**
   * Peeks all values currently available in the [Queue] or returns an empty list.
   */
  fun peekAll(): Kind<F, List<A>>
}

/** A polymorphic effect typeclass that allows [Enqueue]'ing values from a [Queue]. */
interface Enqueue<F, A> {

  /**
   * Offers a value to the [Queue], and might behave differently depending on the [Queue.BackpressureStrategy].
   *
   *  - Semantically blocks until room available in [Queue] for [Queue.BackpressureStrategy.Bounded]
   *  - Returns immediately and slides values through the [Queue] for [Queue.BackpressureStrategy.Sliding]
   *  - Returns immediately and drops values from the [Queue] for [Queue.BackpressureStrategy.Dropping]
   *  - Returns immediately and always offers to the [Queue] for [Queue.BackpressureStrategy.Unbounded]
   *
   *  @see [tryOffer] for a [Queue] that always returns immediately, and returns [true] if the value was succesfully put into the [Queue].
   */
  fun offer(a: A): Kind<F, Unit>

  /**
   * Tries to offer a value to the [Queue], it ignores the [Queue.BackpressureStrategy]
   * and returns false if the [Queue.BackpressureStrategy] does not have room for the value.
   *
   * Use [tryOffer] if you do not want to block or lose a value and return immediately.
   */
  fun tryOffer(a: A): Kind<F, Boolean>

  fun tryOfferAll(a: Collection<A>): Kind<F, Boolean>

  fun tryOfferAll(vararg a: A): Kind<F, Boolean> =
    tryOfferAll(a.toList())

  fun offerAll(a: Collection<A>): Kind<F, Unit>

  fun offerAll(vararg a: A): Kind<F, Unit> =
    offerAll(a.toList())
}

/**
 * Lightweight [Concurrent] [F] [Queue] for values of [A].
 *
 * A [Queue] can be used using 4 different back-pressure strategies:
 *
 *  - [BackpressureStrategy.Bounded]: Offering to a bounded queue at capacity will cause the fiber making
 *   the call to be suspended until the queue has space to receive the offer value
 *
 *  - [BackpressureStrategy.Dropping]: Offering to a dropping queue at capacity will cause the offered
 *   value to be discarded
 *
 *  - [BackpressureStrategy.Sliding]: Offering to a sliding queue at capacity will cause the value at the
 *   front of the queue to be discarded to make room for the offered value
 *
 * - [BackpressureStrategy.Unbounded]: An unbounded queue has no notion of capacity and is bound only by
 *   exhausting the memory limits of the runtime
 */
interface Queue<F, A> : QueueOf<F, A>, Dequeue<F, A>, Enqueue<F, A> {

  /**
   * Immediately returns the current size of values in the [Queue].
   * Can be a negative number when there are takers but no values available.
   */
  fun size(): Kind<F, Int>

  /**
   * Semantically blocks until the [Queue] is [shutdown].
   * Useful for registering hooks that need to be triggered when the [Queue] shuts down.
   */
  fun awaitShutdown(): Kind<F, Unit>

  /**
   * Shut down the [Queue].
   * Shuts down all [offer], [take], [peek] with [QueueShutdown],
   * and call all the registered [awaitShutdown] hooks.
   */
  fun shutdown(): Kind<F, Unit>

  companion object {
    private fun <F> Concurrent<F>.ensureCapacity(capacity: Int): Kind<F, Int> =
      just(capacity).ensure(
        { IllegalArgumentException("Queue must have a capacity greater than 0") },
        { it > 0 }
      )

    /**
     * Create a [Queue] with [BackpressureStrategy.Bounded].
     *
     * Offering to a bounded queue at capacity will cause the fiber making
     * the call to be suspended until the queue has space to receive the offer value.
     */
    fun <F, A> bounded(capacity: Int, CF: Concurrent<F>): Kind<F, Queue<F, A>> = CF.run {
      ensureCapacity(capacity).map { n ->
        ConcurrentQueue<F, A>(Queue.BackpressureStrategy.Bounded(n), ConcurrentQueue.State.empty(), CF)
      }
    }

    /**
     * Create a [Queue] with [BackpressureStrategy.Sliding].
     *
     * Offering to a sliding queue at capacity will cause the value at the
     * front of the queue to be discarded to make room for the offered value
     */
    fun <F, A> sliding(capacity: Int, CF: Concurrent<F>): Kind<F, Queue<F, A>> = CF.run {
      ensureCapacity(capacity).map { n ->
        ConcurrentQueue<F, A>(Queue.BackpressureStrategy.Sliding(n), ConcurrentQueue.State.empty(), CF)
      }
    }

    /**
     * Create a [Queue] with [BackpressureStrategy.Dropping].
     *
     * Offering to a dropping queue at capacity will cause the offered value to be discarded.
     */
    fun <F, A> dropping(capacity: Int, CF: Concurrent<F>): Kind<F, Queue<F, A>> = CF.run {
      ensureCapacity(capacity).map { n ->
        ConcurrentQueue<F, A>(Queue.BackpressureStrategy.Dropping(n), ConcurrentQueue.State.empty(), CF)
      }
    }

    /**
     * Create a [Queue] with [BackpressureStrategy.Unbounded].
     *
     * An unbounded queue has no notion of capacity and is bound only by exhausting the memory limits of the runtime
     */
    fun <F, A> unbounded(CF: Concurrent<F>): Kind<F, Queue<F, A>> = CF.later {
      ConcurrentQueue<F, A>(Queue.BackpressureStrategy.Unbounded, ConcurrentQueue.State.empty(), CF)
    }

    fun <F> factory(CF: Concurrent<F>): QueueFactory<F> =
      QueueFactory(CF)
  }

  /** Internal model that represent the Queue strategies **/
  sealed class BackpressureStrategy {
    data class Bounded(val capacity: Int) : BackpressureStrategy()
    data class Sliding(val capacity: Int) : BackpressureStrategy()
    data class Dropping(val capacity: Int) : BackpressureStrategy()
    object Unbounded : BackpressureStrategy()
  }
}

object QueueShutdown : RuntimeException() {
  override fun fillInStackTrace(): Throwable = this
}

/**
 * Builds a [QueueFactory] for data type [F] without fixing the [Queue]'s [A] type or the [Queue.BackpressureStrategy].
 *
 * ```kotlin:ank:playground
 * import arrow.fx.*
 * import arrow.fx.extensions.fx
 * import arrow.fx.extensions.io.concurrent.concurrent
 *
 * //sampleStart
 * suspend fun main(): Unit = IO.fx {
 *   val factory: QueueFactory<ForIO> = Queue.factory(IO.concurrent())
 *   val unbounded = !factory.unbounded<Int>()
 *   val bounded = !factory.bounded<String>(10)
 *   val sliding = !factory.sliding<Double>(4)
 *   val dropping = !factory.dropping<Float>(4)
 * }.suspended()
 * //sampleEnd
 * ```
 */
interface QueueFactory<F> {

  fun CF(): Concurrent<F>

  /**
   * Create a [Queue] with [BackpressureStrategy.Bounded].
   *
   * Offering to a bounded queue at capacity will cause the fiber making
   * the call to be suspended until the queue has space to receive the offer value.
   */
  fun <A> bounded(capacity: Int): Kind<F, Queue<F, A>> =
    Queue.bounded<F, A>(capacity, CF())

  /**
   * Create a [Queue] with [BackpressureStrategy.Sliding].
   *
   * Offering to a sliding queue at capacity will cause the value at the
   * front of the queue to be discarded to make room for the offered value
   */
  fun <A> sliding(capacity: Int): Kind<F, Queue<F, A>> =
    Queue.sliding(capacity, CF())

  /**
   * Create a [Queue] with [BackpressureStrategy.Dropping].
   *
   * Offering to a dropping queue at capacity will cause the offered value to be discarded.
   */
  fun <A> dropping(capacity: Int): Kind<F, Queue<F, A>> =
    Queue.dropping(capacity, CF())

  /**
   * Create a [Queue] with [BackpressureStrategy.Unbounded].
   *
   * An unbounded queue has no notion of capacity and is bound only by exhausting the memory limits of the runtime
   */
  fun <A> unbounded(): Kind<F, Queue<F, A>> =
    Queue.unbounded(CF())

  companion object {
    operator fun <F> invoke(CF: Concurrent<F>): QueueFactory<F> = object : QueueFactory<F> {
      override fun CF(): Concurrent<F> = CF
    }
  }
}
