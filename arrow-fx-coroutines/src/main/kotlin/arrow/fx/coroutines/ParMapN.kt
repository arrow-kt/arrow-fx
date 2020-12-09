package arrow.fx.coroutines

import arrow.core.Either
import kotlin.coroutines.Continuation
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.intercepted
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn

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
 * Runs [fa], [fb], [fc], [fd] in parallel on [ComputationPool] and combines their results using the provided function.
 * Cancelling this operation cancels all operations running in parallel.
 *
 * @see parMapN for the same function that can run on any [CoroutineContext].
 */
suspend fun <A, B, C, D, E> parMapN(
  fa: suspend () -> A,
  fb: suspend () -> B,
  fc: suspend () -> C,
  fd: suspend () -> D,
  f: (A, B, C, D) -> E
): E = parMapN(ComputationPool, fa, fb, fc, fd, f)

/**
 * Runs [fa], [fb], [fc], [fd], [fe] in parallel on [ComputationPool] and combines their results using the provided function.
 * Cancelling this operation cancels all operations running in parallel.
 *
 * @see parMapN for the same function that can run on any [CoroutineContext].
 */
suspend fun <A, B, C, D, E, F> parMapN(
  fa: suspend () -> A,
  fb: suspend () -> B,
  fc: suspend () -> C,
  fd: suspend () -> D,
  fe: suspend () -> E,
  f: (A, B, C, D, E) -> F
): F = parMapN(ComputationPool, fa, fb, fc, fd, fe, f)

/**
 * Runs [fa], [fb], [fc], [fd], [fe], [ff] in parallel on [ComputationPool] and combines their results using the provided function.
 * Cancelling this operation cancels all operations running in parallel.
 *
 * @see parMapN for the same function that can run on any [CoroutineContext].
 */
suspend fun <A, B, C, D, E, F, G> parMapN(
  fa: suspend () -> A,
  fb: suspend () -> B,
  fc: suspend () -> C,
  fd: suspend () -> D,
  fe: suspend () -> E,
  ff: suspend () -> F,
  f: (A, B, C, D, E, F) -> G
): G = parMapN(ComputationPool, fa, fb, fc, fd, fe, ff, f)

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
      else -> suspend { other.cancel() }.startCoroutineUnintercepted(Continuation(ctx + SuspendConnection.uncancellable) { r ->
        conn.pop()
        cb(Result.failure(r.fold({ e }, { e2 -> Platform.composeErrors(e, e2) })))
      })
    }

    fa.startCoroutineCancellable(CancellableContinuation(ctx, connA) { resA ->
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

    fb.startCoroutineCancellable(CancellableContinuation(ctx, connB) { resB ->
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
  parMapN(
    ctx,
    suspend { parMapN(ctx, fa, fb, ::Pair) },
    fc
  ) { ab, c ->
    val (a, b) = ab
    f(a, b, c)
  }

suspend fun <A, B, C, D, F> parMapN(
  ctx: CoroutineContext,
  fa: suspend () -> A,
  fb: suspend () -> B,
  fc: suspend () -> C,
  fd: suspend () -> D,
  f: (A, B, C, D) -> F
): F =
  parMapN(
    ctx,
    suspend { parMapN(ctx, fa, fb, ::Pair) },
    suspend { parMapN(ctx, fc, fd, ::Pair) }
  ) { ab, cd ->
    val (a, b) = ab
    val (c, d) = cd
    f(a, b, c, d)
  }

suspend fun <A, B, C, D, F, E> parMapN(
  ctx: CoroutineContext,
  fa: suspend () -> A,
  fb: suspend () -> B,
  fc: suspend () -> C,
  fd: suspend () -> D,
  fe: suspend () -> E,
  f: (A, B, C, D, E) -> F
): F =
  parMapN(
    ctx,
    suspend { parMapN(ctx, fa, fb, ::Pair) },
    suspend { parMapN(ctx, fc, fd, ::Pair) },
    fe
  ) { ab, cd, e ->
    val (a, b) = ab
    val (c, d) = cd
    f(a, b, c, d, e)
  }

suspend fun <A, B, C, D, E, F, G> parMapN(
  ctx: CoroutineContext,
  fa: suspend () -> A,
  fb: suspend () -> B,
  fc: suspend () -> C,
  fd: suspend () -> D,
  fe: suspend () -> E,
  ff: suspend () -> F,
  f: (A, B, C, D, E, F) -> G
): G =
  parMapN(
    ctx,
    suspend { parMapN(ctx, fa, fb, ::Pair) },
    suspend { parMapN(ctx, fc, fd, ::Pair) },
    suspend { parMapN(ctx, fe, ff, ::Pair) }
  ) { ab, cd, ef ->
    val (a, b) = ab
    val (c, d) = cd
    val (e, f) = ef
    f(a, b, c, d, e, f)
  }
