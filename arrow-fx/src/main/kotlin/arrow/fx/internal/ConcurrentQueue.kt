package arrow.fx.internal

import arrow.Kind
import arrow.core.Either
import arrow.core.None
import arrow.core.Option
import arrow.core.Right
import arrow.core.Some
import arrow.core.Tuple2
import arrow.core.internal.AtomicRefW
import arrow.core.left
import arrow.fx.Queue
import arrow.fx.Queue.BackpressureStrategy
import arrow.fx.QueueShutdown
import arrow.fx.typeclasses.CancelToken
import arrow.fx.typeclasses.Concurrent
import arrow.fx.typeclasses.rightUnit
import kotlin.coroutines.EmptyCoroutineContext

internal class ConcurrentQueue<F, A> internal constructor(
  private val strategy: BackpressureStrategy,
  initial: State<F, A>?,
  private val CF: Concurrent<F>
) : Concurrent<F> by CF,
  Queue<F, A> {

  private val state: AtomicRefW<State<F, A>> = AtomicRefW(initial ?: State.empty())

  override fun size(): Kind<F, Int> = defer {
    when (val curr = state.value) {
      is State.Deficit -> just(-curr.takes.size)
      is State.Surplus -> just(1 + curr.offers.size)
      State.Shutdown -> raiseError(QueueShutdown)
    }
  }

  override fun tryOffer(a: A): Kind<F, Boolean> =
    defer { unsafeTryOffer(a) }

  override fun offer(a: A): Kind<F, Unit> =
    defer { unsafeTryOffer(a, false) }.flatMap { didPut ->
      if (didPut) unit() else cancelableF { cb -> unsafeOffer(a, cb) }
    }

  override fun offerAll(a: Collection<A>): Kind<F, Unit> =
    TODO()

  override fun tryOfferAll(a: Collection<A>): Kind<F, Boolean> =
    defer { unsafeTryOfferAll(a) }

  override fun tryTake(): Kind<F, Option<A>> =
    defer { unsafeTryTake() }

  override fun take(): Kind<F, A> =
    tryTake().flatMap {
      it.fold({ cancelableF(::unsafeTake) }, ::just)
    }

  override fun takeAll(): Kind<F, List<A>> =
    defer { unsafeTakeAll() }

  override fun tryPeek(): Kind<F, Option<A>> =
    defer { unsafeTryPeek() }

  override fun peek(): Kind<F, A> =
    tryPeek().flatMap {
      it.fold({ cancelableF(::unsafePeek) }, ::just)
    }

  override fun peekAll(): Kind<F, List<A>> =
    defer { unsafePeekAll() }

  /**
   * Waits until the queue is shutdown.
   * The `IO` returned by this method will not resume until the queue has been shutdown.
   * If the queue is already shutdown, the `IO` will resume right away.
   */
  override fun awaitShutdown(): Kind<F, Unit> =
    CF.cancelableF(::unsafeRegisterAwaitShutdown)

  /**
   * Cancels any fibers that are suspended on `offer` or `take`.
   * Future calls to `offer*` and `take*` will be interrupted immediately.
   */
  override fun shutdown(): Kind<F, Unit> =
    defer { unsafeShutdown() }

  private tailrec fun unsafeTryOffer(a: A, tryStrategy: Boolean = true): Kind<F, Boolean> =
    when (val current = state.value) {
      is State.Surplus -> unsafeOfferSurplusForStrategy(a, tryStrategy, current) ?: unsafeTryOffer(a, tryStrategy)
      is State.Deficit -> {
        val firstTake = current.takes.values.firstOrNull()

        val update: State<F, A> = if (firstTake == null) State.Surplus(IQueue(a), linkedMapOf(), current.shutdownHook) else {
          val rest = current.takes.toList().drop(1)
          if (rest.isEmpty()) current.copy(linkedMapOf(), linkedMapOf())
          else State.Deficit(emptyMap(), rest.toMap(), current.shutdownHook)
        }

        if (!state.compareAndSet(current, update)) {
          unsafeTryOffer(a, tryStrategy)
        } else if (firstTake != null || current.reads.isNotEmpty()) {
          callOfferAndPeeks(a, firstTake, current.reads)
        } else just(true)
      }
      is State.Shutdown -> just(false)
    }

  private tailrec fun unsafeTryOfferAll(aas: Collection<A>, tryStrategy: Boolean = true): Kind<F, Boolean> =
    when (val current = state.value) {
      is State.Surplus -> unsafeOfferAllSurplusForStrategy(aas, tryStrategy, current)
        ?: unsafeTryOfferAll(aas, tryStrategy)
      is State.Deficit -> {
        if (aas.size > current.takes.values.size) unsafeOfferAllDecifitForStrategy(aas, tryStrategy, current)
          ?: unsafeTryOfferAll(aas, tryStrategy)
        else {
          val update = if (aas.size == current.takes.values.size) State.empty<F, A>().copy(shutdownHook = current.shutdownHook)
          else current.copy(reads = emptyMap(), takes = current.takes.toList().drop(aas.size).toMap(), shutdownHook = current.shutdownHook)

          if (!state.compareAndSet(current, update)) {
            unsafeTryOfferAll(aas, tryStrategy)
          } else if (current.takes.isNotEmpty() || current.reads.isNotEmpty()) {
            later {
              aas.firstOrNull()?.let { a -> current.reads.values.callAll(Either.Right(a)) }
              aas.zip(current.takes.values) { a, cb ->
                cb(Either.Right(a))
              }

              true
            }
          } else just(true)
        }
      }
      is State.Shutdown -> just(false)
    }

  private tailrec fun unsafeOffer(a: A, onPut: (Either<Throwable, Unit>) -> Unit): Kind<F, CancelToken<F>> =
    when (val current = state.value) {
      is State.Surplus -> {
        val id = Token()
        val newMap = current.offers + Pair(id, Tuple2(a, onPut))
        if (state.compareAndSet(current, State.Surplus(current.value, newMap, current.shutdownHook))) just(later { unsafeCancelOffer(id) })
        else unsafeOffer(a, onPut)
      }
      is State.Deficit -> {
        val first = current.takes.values.firstOrNull()
        val update = if (current.takes.isEmpty()) State.Surplus<F, A>(IQueue(a), linkedMapOf(), current.shutdownHook) else {
          val rest = current.takes.toList().drop(1)
          if (rest.isEmpty()) current.copy(linkedMapOf(), linkedMapOf())
          else State.Deficit(emptyMap(), rest.toMap(), current.shutdownHook)
        }

        if (state.compareAndSet(current, update)) {
          if (first != null || current.reads.isNotEmpty()) callOfferAndPeeks(a, first, current.reads).map {
            onPut(rightUnit)
            unit()
          } else {
            onPut(rightUnit)
            just(unit())
          }
        } else unsafeOffer(a, onPut)
      }
      is State.Shutdown -> {
        onPut(Either.Left(QueueShutdown))
        just(unit())
      }
    }

  private tailrec fun unsafeCancelOffer(id: Token): Unit =
    when (val current = state.value) {
      is State.Surplus -> {
        val update = current.copy(offers = current.offers - id)
        if (state.compareAndSet(current, update)) Unit
        else unsafeCancelOffer(id)
      }
      else -> Unit
    }

  private tailrec fun unsafeTryTake(): Kind<F, Option<A>> =
    when (val current = state.value) {
      is State.Surplus -> {
        val (head, tail) = current.value.dequeue()
        if (current.offers.isEmpty()) {
          val update = if (tail.isEmpty()) State.empty<F, A>().copy(shutdownHook = current.shutdownHook) else current.copy(value = tail)
          if (state.compareAndSet(current, update)) just(Some(head))
          else unsafeTryTake()
        } else {
          val (ax, notify) = current.offers.values.first()
          val xs = current.offers.toList().drop(1)
          if (state.compareAndSet(current, State.Surplus(tail.enqueue(ax), xs.toMap(), current.shutdownHook))) later { notify(rightUnit) }.fork(EmptyCoroutineContext).map { Some(head) }
          else unsafeTryTake()
        }
      }
      is State.Deficit -> just(None)
      is State.Shutdown -> just(None)
    }

  private tailrec fun unsafeTake(onTake: (Either<Throwable, A>) -> Unit): Kind<F, CancelToken<F>> =
    when (val current = state.value) {
      is State.Surplus -> {
        val (head, tail) = current.value.dequeue()
        if (current.offers.isEmpty()) {
          val update = if (tail.isEmpty()) State.empty<F, A>().copy(shutdownHook = current.shutdownHook) else current.copy(value = tail)
          if (state.compareAndSet(current, update)) {
            onTake(Right(head))
            just(unit())
          } else {
            unsafeTake(onTake)
          }
        } else {
          val (ax, notify) = current.offers.values.first()
          val xs = current.offers.toList().drop(0)
          if (state.compareAndSet(current, State.Surplus(tail.enqueue(ax), xs.toMap(), current.shutdownHook))) {
            later { notify(rightUnit) }.fork(EmptyCoroutineContext).map {
              onTake(Either.Right(head))
              unit()
            }
          } else unsafeTake(onTake)
        }
      }
      is State.Deficit -> {
        val id = Token()
        val newQueue = current.takes + Pair(id, onTake)
        if (state.compareAndSet(current, State.Deficit(current.reads, newQueue, current.shutdownHook))) just(later { unsafeCancelTake(id) })
        else unsafeTake(onTake)
      }
      is State.Shutdown -> {
        onTake(Either.Left(QueueShutdown))
        just(unit())
      }
    }

  private tailrec fun unsafeCancelTake(id: Token): Unit =
    when (val current = state.value) {
      is State.Deficit -> {
        val newMap = current.takes - id
        val update = State.Deficit<F, A>(current.reads, newMap, current.shutdownHook)
        if (state.compareAndSet(current, update)) Unit
        else unsafeCancelTake(id)
      }
      else -> Unit
    }

  private tailrec fun unsafeTakeAll(): Kind<F, List<A>> =
    when (val current = state.value) {
      is State.Surplus -> {
        val all = current.value.toList()
        if (current.offers.isEmpty()) {
          if (state.compareAndSet(current, State.empty<F, A>().copy(shutdownHook = current.shutdownHook))) just(all)
          else unsafeTakeAll()
        } else {
          val allValues = current.offers.values.map { it.a }
          val allOffers = current.offers.values.map { it.b }
          if (state.compareAndSet(current, State.empty<F, A>().copy(shutdownHook = current.shutdownHook))) later { allOffers.forEach { cb -> cb(rightUnit) } }.map { all + allValues }
          else unsafeTakeAll()
        }
      }
      is State.Deficit -> just(emptyList<A>())
      is State.Shutdown -> raiseError(QueueShutdown)
    }

  private fun unsafePeekAll(): Kind<F, List<A>> =
    when (val current = state.value) {
      is State.Deficit -> just(emptyList())
      is State.Surplus -> {
        val all = current.value.toList()
        val allOffered = current.offers.values.map { it.a }
        just(all + allOffered)
      }
      is State.Shutdown -> raiseError(QueueShutdown)
    }

  private fun unsafeTryPeek(): Kind<F, Option<A>> =
    when (val current = state.value) {
      is State.Surplus -> just(Some(current.value.head()))
      is State.Deficit -> just(None)
      is State.Shutdown -> just(None)
    }

  private tailrec fun unsafePeek(onPeek: (Either<Throwable, A>) -> Unit): Kind<F, CancelToken<F>> =
    when (val current = state.value) {
      is State.Surplus -> {
        onPeek(Right(current.value.head()))
        just(unit())
      }
      is State.Deficit -> {
        val id = Token()
        val newReads = current.reads + Pair(id, onPeek)
        if (state.compareAndSet(current, State.Deficit(newReads, current.takes, current.shutdownHook))) just(later { unsafeCancelRead(id) })
        else unsafePeek(onPeek)
      }
      is State.Shutdown -> {
        onPeek(Either.Left(QueueShutdown))
        just(unit())
      }
    }

  private tailrec fun unsafeCancelRead(id: Token): Unit =
    when (val current = state.value) {
      is State.Deficit -> {
        val newMap = current.reads - id
        val update = State.Deficit<F, A>(newMap, current.takes, current.shutdownHook)
        if (state.compareAndSet(current, update)) Unit
        else unsafeCancelRead(id)
      }
      else -> Unit
    }

  private tailrec fun unsafeRegisterAwaitShutdown(shutdown: (Either<Throwable, Unit>) -> Unit): Kind<F, CancelToken<F>> =
    when (val curr = state.value) {
      is State.Deficit -> {
        val token = Token()
        val newShutdowns = curr.shutdownHook + Pair(token, shutdown)
        if (state.compareAndSet(curr, curr.copy(shutdownHook = newShutdowns))) just(later { unsafeCancelAwaitShutdown(token) })
        else unsafeRegisterAwaitShutdown(shutdown)
      }
      is State.Surplus -> {
        val token = Token()
        val newShutdowns = curr.shutdownHook + Pair(token, shutdown)
        if (state.compareAndSet(curr, curr.copy(shutdownHook = newShutdowns))) just(later { unsafeCancelAwaitShutdown(token) })
        else unsafeRegisterAwaitShutdown(shutdown)
      }
      State.Shutdown -> {
        shutdown(rightUnit)
        just(CF.unit())
      }
    }

  private tailrec fun unsafeCancelAwaitShutdown(id: Token): Unit =
    when (val curr = state.value) {
      is State.Deficit -> {
        val update = curr.copy(shutdownHook = curr.shutdownHook - id)
        if (state.compareAndSet(curr, update)) Unit
        else unsafeCancelAwaitShutdown(id)
      }
      is State.Surplus -> {
        val update = curr.copy(shutdownHook = curr.shutdownHook - id)
        if (state.compareAndSet(curr, update)) Unit
        else unsafeCancelAwaitShutdown(id)
      }
      else -> Unit
    }

  private tailrec fun unsafeShutdown(): Kind<F, Unit> =
    when (val current = state.value) {
      is State.Shutdown -> raiseError(QueueShutdown)
      is State.Surplus ->
        if (state.compareAndSet(current, State.shutdown())) later {
          current.offers.values.forEach { (_, cb) -> cb(QueueShutdown.left()) }
          current.shutdownHook.values.forEach { cb -> cb(rightUnit) }
        } else unsafeShutdown()
      is State.Deficit ->
        if (state.compareAndSet(current, State.shutdown())) later {
          current.takes.forEach { (_, cb) -> cb(QueueShutdown.left()) }
          current.reads.forEach { (_, cb) -> cb(QueueShutdown.left()) }
          current.shutdownHook.values.forEach { cb -> cb(rightUnit) }
        } else unsafeShutdown()
    }

  private fun callOfferAndPeeks(a: A, put: ((Either<Nothing, A>) -> Unit)?, peeks: Map<Token, (Either<Nothing, A>) -> Unit>): Kind<F, Boolean> {
    val value = Right(a)
    return peeks.values.callAll(value).flatMap {
      if (put != null) later { put(value) }.map { true }
      else just(true)
    }
  }

  // For streaming a value to a whole `reads` collection
  private fun Iterable<(Either<Nothing, A>) -> Unit>.callAll(value: Either<Nothing, A>): Kind<F, Unit> =
    later { forEach { cb -> cb(value) } }

  // fold(null as Kind<F, Fiber<F, Unit>>?) { acc, cb ->
  //   val task = later { cb(value) }.fork(EmptyCoroutineContext)
  //   acc?.flatMap { task } ?: task
  // }?.map(mapUnit) ?: unit()

  /**
   * A Queue can be in three states
   * [Deficit]: Contains three maps of registered id & take/reads/shutdown callbacks waiting
   *   for a value to become available.
   *
   * [Surplus]: Contains a queue of values and two maps of registered id & offer/shutdown callbacks waiting to
   *   offer once there is room (if the queue is bounded, dropping or sliding).
   *
   * [Shutdown]: Holds no values, an offer or take in Shutdown state creates a QueueShutdown error.
   */
  sealed class State<F, out A> {
    data class Deficit<F, A>(
      val reads: Map<Token, (Either<Throwable, A>) -> Unit>,
      val takes: Map<Token, (Either<Throwable, A>) -> Unit>,
      val shutdownHook: Map<Token, (Either<Throwable, Unit>) -> Unit>
    ) : State<F, A>()

    data class Surplus<F, A>(
      val value: IQueue<A>,
      val offers: Map<Token, Tuple2<A, (Either<Throwable, Unit>) -> Unit>>,
      val shutdownHook: Map<Token, (Either<Throwable, Unit>) -> Unit>
    ) : State<F, A>()

    object Shutdown : State<Any?, Nothing>()

    companion object {
      private val empty: Deficit<Any?, Any?> = Deficit(linkedMapOf(), linkedMapOf(), linkedMapOf())
      fun <F, A> empty(): Deficit<F, A> = empty as Deficit<F, A>
      fun <F, A> shutdown(): State<F, A> =
        Shutdown as State<F, A>
    }
  }

  /**
   * Unsafely handle offer at State.Surplus.
   *
   * Return:
   *  - `just(true)` when handled
   *  - `just(false)` when offer should be scheduled (only used for bounded).
   *     or when [tryStrategy] is true to signal no room in the [Queue].
   *  - null when needs to recurse and try again
   */
  private fun unsafeOfferSurplusForStrategy(a: A, tryStrategy: Boolean, surplus: State.Surplus<F, A>): Kind<F, Boolean>? =
    when (strategy) {
      is BackpressureStrategy.Bounded ->
        when {
          surplus.value.length() >= strategy.capacity -> just(false)
          state.compareAndSet(surplus, surplus.copy(surplus.value.enqueue(a))) -> just(true)
          else -> null
        }
      is BackpressureStrategy.Sliding -> {
        val nextQueue = if (surplus.value.length() < strategy.capacity) surplus.value.enqueue(a) else surplus.value.dequeue().b.enqueue(a)

        when {
          surplus.value.length() >= strategy.capacity && tryStrategy -> just(false)
          state.compareAndSet(surplus, surplus.copy(value = nextQueue)) -> just(true)
          else -> null
        }
      }
      is BackpressureStrategy.Dropping -> {
        val nextQueue = if (surplus.value.length() < strategy.capacity) surplus.value.enqueue(a) else surplus.value

        when {
          surplus.value.length() >= strategy.capacity && tryStrategy -> just(false)
          state.compareAndSet(surplus, surplus.copy(value = nextQueue)) -> just(true)
          else -> null
        }
      }
      is BackpressureStrategy.Unbounded ->
        if (!state.compareAndSet(surplus, surplus.copy(value = surplus.value.enqueue(a)))) null
        else just(true)
    }

  private fun unsafeOfferAllSurplusForStrategy(a: Collection<A>, tryStrategy: Boolean, surplus: State.Surplus<F, A>): Kind<F, Boolean>? =
    when (strategy) {
      is BackpressureStrategy.Bounded ->
        when {
          surplus.value.length() + a.size > strategy.capacity -> just(false)
          state.compareAndSet(surplus, surplus.copy(surplus.value.enqueue(a))) -> just(true)
          else -> null
        }
      is BackpressureStrategy.Sliding -> {
        val nextQueue = if (surplus.value.length() + (a.size - 1) < strategy.capacity) surplus.value.enqueue(a)
        else surplus.value.drop(a.size).enqueue(a)

        when {
          surplus.value.length() >= strategy.capacity && tryStrategy -> just(false)
          state.compareAndSet(surplus, surplus.copy(value = nextQueue)) -> just(true)
          else -> null
        }
      }
      is BackpressureStrategy.Dropping -> {
        val nextQueue = if (surplus.value.length() + (a.size - 1) < strategy.capacity) surplus.value.enqueue(a)
        else surplus.value

        when {
          surplus.value.length() >= strategy.capacity && tryStrategy -> just(false)
          state.compareAndSet(surplus, surplus.copy(value = nextQueue)) -> just(true)
          else -> null
        }
      }
      is BackpressureStrategy.Unbounded ->
        if (!state.compareAndSet(surplus, surplus.copy(value = surplus.value.enqueue(a)))) null
        else just(true)
    }

  private fun unsafeOfferAllDecifitForStrategy(a: Collection<A>, tryStrategy: Boolean, surplus: State.Deficit<F, A>): Kind<F, Boolean>? =
    when (strategy) {
      is BackpressureStrategy.Bounded ->
        when {
          // We need to atomically offer to deficit AND schedule offers according to capacity.
          a.size > surplus.takes.size && (a.size - surplus.takes.size) > strategy.capacity -> just(false)

          a.size > surplus.takes.size -> { //Offer to deficit and update to Surplus, capacity check above
            val update = State.Surplus<F, A>(IQueue(a.drop(surplus.takes.size)), emptyMap(), surplus.shutdownHook)

            if (state.compareAndSet(surplus, update)) later {
              a.firstOrNull()?.let { Either.Right(it) }?.let { a ->
                surplus.reads.forEach { (_, cb) -> cb(a) }
              }
              a.zip(surplus.takes.toList().take(a.size)) { a, (_, cb) ->
                println("Offered $a")
                cb(Either.Right(a))
              }
              true
            } else null // recurse
          }

          // a.size <= surplus.takes.size -> {// We have a bigger deficit than available values, we can atomically offer.
          //   val update = surplus.copy(reads = emptyMap(), takes = surplus.takes.toList().drop(a.size).toMap())
          //   if (state.compareAndSet(surplus, update)) later {
          //     a.firstOrNull()?.let { Either.Right(it) }?.let { a ->
          //       surplus.reads.forEach { (_, cb) -> cb(a) }
          //     }
          //     a.zip(surplus.takes.toList().take(a.size)) { a, (_, cb) ->
          //       cb(Either.Right(a))
          //     }
          //
          //     true
          //   } else null // update failed, recurse
          // }
          else -> null // updates failed, recurse
        }
      is BackpressureStrategy.Sliding -> {
        if ((a.size - surplus.takes.size) > strategy.capacity) just(false)
        else {
          val update = State.Surplus<F, A>(IQueue(a.drop(surplus.takes.size)), emptyMap(), surplus.shutdownHook)

          if (state.compareAndSet(surplus, update)) later {
            a.firstOrNull()?.let { Either.Right(it) }?.let { a ->
              surplus.reads.forEach { (_, cb) -> cb(a) }
            }
            a.zip(surplus.takes.toList().take(a.size)) { a, (_, cb) ->
              cb(Either.Right(a))
            }
            true
          } else null // recurse
        }

        // val update = surplus.copy(reads = emptyMap(), takes = surplus.takes.toList().drop(a.size).toMap())

        // TODO()
        // when {
        //   surplus.value.length() >= strategy.capacity && tryStrategy -> just(false)
        //   state.compareAndSet(surplus, surplus.copy(value = nextQueue)) -> just(true)
        //   else -> null
        // }
      }
      is BackpressureStrategy.Dropping -> {
        if ((a.size - surplus.takes.size) > strategy.capacity) just(false)
        else {
          val update = State.Surplus<F, A>(IQueue(a.drop(surplus.takes.size)), emptyMap(), surplus.shutdownHook)

          if (state.compareAndSet(surplus, update)) later {
            a.firstOrNull()?.let { Either.Right(it) }?.let { a ->
              surplus.reads.forEach { (_, cb) -> cb(a) }
            }
            a.zip(surplus.takes.toList().take(a.size)) { a, (_, cb) ->
              cb(Either.Right(a))
            }
            true
          } else null // recurse
        }
      }
      is BackpressureStrategy.Unbounded -> {
        val update = State.Surplus<F, A>(IQueue(a.drop(surplus.takes.size)), emptyMap(), surplus.shutdownHook)

        if (state.compareAndSet(surplus, update)) later {
          a.firstOrNull()?.let { Either.Right(it) }?.let { a ->
            surplus.reads.forEach { (_, cb) -> cb(a) }
          }

          a.zip(surplus.takes.toList().take(a.size)) { a, (_, cb) ->
            cb(Either.Right(a))
          }

          true
        } else null
      }

    }

  companion object {
    fun <F, A> empty(CF: Concurrent<F>): Kind<F, Queue<F, A>> = CF.later {
      ConcurrentQueue<F, A>(BackpressureStrategy.Unbounded, null, CF)
    }
  }
}
