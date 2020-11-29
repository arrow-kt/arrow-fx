package arrow.fx.coroutines

import arrow.core.Either
import kotlin.coroutines.Continuation
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.intercepted
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlin.coroutines.startCoroutine

/**
 * Runs [fa], [fb] in parallel on [ComputationPool] and combines their results using the provided function.
 * Cancelling this operation cancels both operations running in parallel.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.coroutines.*
 *
 * suspend fun main(): Unit {
 *   //sampleStart
 *   val result = parMapN(
 *     { "First one is on ${Thread.currentThread().name}" },
 *     { "Second one is on ${Thread.currentThread().name}" }
 *   ) { a, b ->
 *       "$a\n$b"
 *     }
 *   //sampleEnd
 *  println(result)
 * }
 * ```
 *
 * @param fa value to parallel map
 * @param fb value to parallel map
 * @param f function to map/combine value [A] and [B]
 * ```
 *
 * @see parMapN for the same function that can race on any [CoroutineContext].
 */
suspend fun <A, B, C> parMapN(fa: suspend () -> A, fb: suspend () -> B, f: (A, B) -> C): C =
  parMapN(ComputationPool, fa, fb, f)

/**
 * Runs [fa], [fb], [fc] in parallel on [ComputationPool] and combines their results using the provided function.
 * Cancelling this operation cancels both operations running in parallel.
 *
 * @see parMapN for the same function that can run on any [CoroutineContext].
 */
suspend fun <A, B, C, D> parMapN(
  fa: suspend () -> A,
  fb: suspend () -> B,
  fc: suspend () -> C,
  f: (A, B, C) -> D
): D = parMapN(ComputationPool, fa, fb, fc, f)

/**
 * Runs [fa], [fb] on the provided [CoroutineContext] and combines their results using the provided function.
 * Cancelling this operation cancels both tasks running in parallel.
 *
 * **WARNING** this function forks [fa], [fb] but if they run in parallel depends
 * on the capabilities of the provided [CoroutineContext].
 * We ensure they start in sequence so it's guaranteed to finish on a single threaded context.
 *
 * @see parMapN for a function that ensures it runs in parallel on the [ComputationPool].
 */
@Suppress("UNCHECKED_CAST")
suspend fun <A, B, C> parMapN(
  ctx: CoroutineContext,
  fa: suspend () -> A,
  fb: suspend () -> B,
  f: (A, B) -> C
): C =
  suspendCoroutineUninterceptedOrReturn { _cont ->
    val conn = _cont.context[SuspendConnection] ?: SuspendConnection.uncancellable
    val cont = _cont.intercepted()
    val cb = cont::resumeWith

    // Used to store Throwable, Either<A, B> or empty (null). (No sealed class used for a slightly better performing ParMap2)
    val state = AtomicRefW<Any?>(null)

    val connA = SuspendConnection()
    val connB = SuspendConnection()

    conn.pushPair(connA, connB)

    fun complete(a: A, b: B) {
      conn.pop()
      cb(
        try {
          Result.success(f(a, b))
        } catch (e: Throwable) {
          Result.failure<C>(e.nonFatalOrThrow())
        }
      )
    }

    fun sendException(other: SuspendConnection, e: Throwable) = when (state.getAndSet(e)) {
      is Throwable -> Unit // Do nothing we already finished
      else -> suspend { other.cancel() }.startCoroutine(Continuation(EmptyCoroutineContext) { r ->
        conn.pop()
        cb(Result.failure(r.fold({ e }, { e2 -> Platform.composeErrors(e, e2) })))
      })
    }

    fa.startCoroutineCancellable(FiberContinuation(ctx, connA) { resA ->
      resA.fold({ a ->
        when (val oldState = state.getAndSet(Either.Left(a))) {
          null -> Unit // Wait for B
          is Throwable -> Unit // ParMapN already failed and A was cancelled.
          is Either.Left<*> -> Unit // Already state.getAndSet
          is Either.Right<*> -> complete(a, (oldState as Either.Right<B>).b)
        }
      }, { e ->
        sendException(connB, e)
      })
    })

    fb.startCoroutineCancellable(FiberContinuation(ctx, connB) { resB ->
      resB.fold({ b ->
        when (val oldState = state.getAndSet(Either.Right(b))) {
          null -> Unit // Wait for A
          is Throwable -> Unit // ParMapN already failed and B was cancelled.
          is Either.Right<*> -> Unit // IO cannot finish twice
          is Either.Left<*> -> complete((oldState as Either.Left<A>).a, b)
        }
      }, { e ->
        sendException(connA, e)
      })
    })

    COROUTINE_SUSPENDED
  }

/**
 * Runs [fa], [fb], [fc] on the provided [CoroutineContext] and combines their results using the provided function.
 * Cancelling this operation cancels both tasks running in parallel.
 *
 * **WARNING**: this function forks [fa], [fb], [fc] but if they run in parallel depends
 * on the capabilities of the provided [CoroutineContext].
 * We ensure they start in sequence so it's guaranteed to finish on a single threaded context.
 *
 * @see parMapN for a function that ensures it runs in parallel on the [ComputationPool].
 */
suspend fun <A, B, C, D> parMapN(
  ctx: CoroutineContext,
  fa: suspend () -> A,
  fb: suspend () -> B,
  fc: suspend () -> C,
  f: (A, B, C) -> D
): D =
  suspendCoroutineUninterceptedOrReturn { cont ->
    val conn = cont.context[SuspendConnection] ?: SuspendConnection.uncancellable
    val cont = cont.intercepted()
    val cb = cont::resumeWith

    val state: AtomicRefW<Triple<A?, B?, C?>?> = AtomicRefW(null)
    val active = AtomicBooleanW(true)

    val connA = SuspendConnection()
    val connB = SuspendConnection()
    val connC = SuspendConnection()

    conn.push(listOf(suspend { connA.cancel() }, suspend { connB.cancel() }, suspend { connC.cancel() }))

    fun complete(a: A, b: B, c: C) {
      conn.pop()
      cb(Result.success(f(a, b, c)))
    }

    fun tryComplete(result: Triple<A?, B?, C?>?): Unit {
      result?.let { (a, b, c) ->
        a?.let {
          b?.let {
            c?.let {
              complete(a, b, c)
            }
          }
        }
      } ?: Unit
    }

    fun sendException(other: SuspendConnection, other2: SuspendConnection, e: Throwable) =
      if (active.getAndSet(false)) { // We were already cancelled so don't do anything.
        suspend { other.cancel() }.startCoroutine(Continuation(EmptyCoroutineContext) { r1 ->
          suspend { other2.cancel() }.startCoroutine(Continuation(EmptyCoroutineContext) { r2 ->
            conn.pop()
            cb(Result.failure(r1.fold({
              r2.fold({ e }, { e3 -> Platform.composeErrors(e, e3) })
            }, { e2 ->
              r2.fold({ Platform.composeErrors(e, e2) }, { e3 -> Platform.composeErrors(e, e2, e3) })
            })))
          })
        })
      } else Unit

    fa.startCoroutineCancellable(FiberContinuation(ctx, connA) { resA ->
      resA.fold({ a ->
        val newState = state.updateAndGet { current ->
          current?.copy(first = a) ?: Triple(a, null, null)
        }
        tryComplete(newState)
      }, { e ->
        sendException(connB, connC, e)
      })
    })

    fb.startCoroutineCancellable(FiberContinuation(ctx, connB) { resB ->
      resB.fold({ b ->
        val newState = state.updateAndGet { current ->
          current?.copy(second = b) ?: Triple(null, b, null)
        }
        tryComplete(newState)
      }, { e ->
        sendException(connA, connC, e)
      })
    })

    fc.startCoroutineCancellable(FiberContinuation(ctx, connC) { resC ->
      resC.fold({ c ->
        val newState = state.updateAndGet { current ->
          current?.copy(third = c) ?: Triple(null, null, c)
        }
        tryComplete(newState)
      }, { e ->
        sendException(connA, connB, e)
      })
    })

    COROUTINE_SUSPENDED
  }
