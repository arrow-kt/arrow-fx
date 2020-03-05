package arrow.fx

import arrow.Kind
import arrow.Kind2
import arrow.core.Tuple2
import arrow.fx.internal.CancellableQueue
import arrow.fx.internal.IQueue
import arrow.fx.typeclasses.Concurrent
import arrow.typeclasses.Applicative
import arrow.typeclasses.ApplicativeError

class ForQueue private constructor() {
  companion object
}

typealias QueueOf<F, A> = Kind2<ForQueue, F, A>
typealias QueuePartialOf<F> = Kind<ForQueue, F>

@Suppress("UNCHECKED_CAST", "NOTHING_TO_INLINE")
inline fun <F, A> QueueOf<F, A>.fix(): Queue<F, A> =
  this as Queue<F, A>

interface Dequeue<F, A> {
  fun take(): Kind<F, A>
}

interface Enqueue<F, A> {
  fun offer(a: A): Kind<F, Unit>
}

interface Queue<F, A> :
  QueueOf<F, A>,
  Dequeue<F, A>,
  Enqueue<F, A> {

  fun size(): Kind<F, Int>
  fun awaitShutdown(): Kind<F, Unit>
  fun shutdown(): Kind<F, Unit>

  /**
   * A Queue can be in three states
   * Deficit:
   *  Contains a queue of values and a queue of suspended fibers
   *  waiting to take once a value becomes available
   * Surplus:
   *  Contains a queue of values and a queue of suspended fibers
   *  waiting to offer once there is room (if the queue is bounded)
   * Shutdown:
   *  Holds no values or promises for suspended calls,
   *  an offer or take in Shutdown state creates a QueueShutdown error
   */
  sealed class State<F, out A> {
    abstract fun size(): Kind<F, Int>

    data class Deficit<F, A>(val takers: IQueue<Promise<F, A>>, val AP: Applicative<F>, val shutdownHook: Kind<F, Unit>) : State<F, A>() {
      override fun size(): Kind<F, Int> = AP.just(-takers.length())
    }

    data class Surplus<F, A>(val queue: IQueue<A>, val putters: IQueue<Tuple2<A, Promise<F, Unit>>>, val AP: Applicative<F>, val shutdownHook: Kind<F, Unit>) : State<F, A>() {
      override fun size(): Kind<F, Int> = AP.just(queue.length() + putters.length())
    }

    data class Shutdown<F>(val AE: ApplicativeError<F, Throwable>) : State<F, Nothing>() {
      override fun size(): Kind<F, Int> = AE.raiseError(QueueShutdown)
    }
  }

  companion object {
    private fun <F> ensureCapacity(capacity: Int, CF: Concurrent<F>): Kind<F, Int> = CF.run {
      just(capacity).ensure(
        { IllegalArgumentException("Sliding queue must have a capacity greater than 0") },
        { it > 0 }
      )
    }

    /**
     * A Queue can be in three states
     * Deficit:
     *  Contains a queue of values and a queue of suspended fibers
     *  waiting to take once a value becomes available
     * Surplus:
     *  Contains a queue of values and a queue of suspended fibers
     *  waiting to offer once there is room (if the queue is bounded)
     * Shutdown:
     *  Holds no values or promises for suspended calls,
     *  an offer or take in Shutdown state creates a QueueShutdown error
     */
    fun <F, A> bounded(capacity: Int, CF: Concurrent<F>): Kind<F, Queue<F, A>> = CF.run {
      ensureCapacity(capacity, CF).map {
        CancellableQueue<F, A>(CancellableQueue.Companion.State.empty(), CancellableQueue.SurplusStrategy.Bounded(capacity, CF), CF)
      }
    }

    fun <F, A> sliding(capacity: Int, CF: Concurrent<F>): Kind<F, Queue<F, A>> = CF.run {
      ensureCapacity(capacity, CF).map { n ->
        CancellableQueue<F, A>(CancellableQueue.Companion.State.empty(), CancellableQueue.SurplusStrategy.Sliding(n, CF), CF)
      }
    }

    fun <F, A> dropping(capacity: Int, CF: Concurrent<F>): Kind<F, Queue<F, A>> = CF.run {
      ensureCapacity(capacity, CF).map {
        CancellableQueue<F, A>(CancellableQueue.Companion.State.empty(), CancellableQueue.SurplusStrategy.Dropping(capacity, CF), CF)
      }
    }

    fun <F, A> unbounded(CF: Concurrent<F>): Kind<F, Queue<F, A>> = CF.later {
      CancellableQueue<F, A>(CancellableQueue.Companion.State.empty(), CancellableQueue.SurplusStrategy.Unbounded(CF), CF)
    }
  }
}

object QueueShutdown : RuntimeException() {
  override fun fillInStackTrace(): Throwable = this
}
