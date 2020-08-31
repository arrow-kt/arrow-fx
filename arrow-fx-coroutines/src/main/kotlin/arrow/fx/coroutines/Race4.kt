package arrow.fx.coroutines

import kotlin.coroutines.Continuation
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.startCoroutine
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlin.coroutines.intrinsics.intercepted
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED

sealed class Race4<out A, out B, out C, out D> {
  data class First<A>(val winner: A) : Race4<A, Nothing, Nothing, Nothing>()
  data class Second<B>(val winner: B) : Race4<Nothing, B, Nothing, Nothing>()
  data class Third<C>(val winner: C) : Race4<Nothing, Nothing, C, Nothing>()
  data class Fourth<D>(val winner: D) : Race4<Nothing, Nothing, Nothing, D>()

  inline fun <E> fold(
    ifA: (A) -> E,
    ifB: (B) -> E,
    ifC: (C) -> E,
    ifD: (D) -> E
  ): E = when (this) {
    is First -> ifA(winner)
    is Second -> ifB(winner)
    is Third -> ifC(winner)
    is Fourth -> ifD(winner)
  }
}

/**
 * Races the participants [fa], [fb], [fc], and [fd] in parallel on the [ComputationPool].
 * The winner of the race cancels the other participants,
 * cancelling the operation cancels all participants.
 *
 * @see raceN for the same function that can race on any [CoroutineContext].
 */
suspend fun <A, B, C, D> raceN(
  fa: suspend () -> A,
  fb: suspend () -> B,
  fc: suspend () -> C,
  fd: suspend () -> D
): Race4<A, B, C, D> = raceN(ComputationPool, fa, fb, fc, fd)

/**
 * Races the participants [fa], [fb], [fc], and [fd] on the provided [CoroutineContext].
 * The winner of the race cancels the other participants,
 * cancelling the operation cancels all participants.
 *
 * **WARNING** it runs in parallel depending on the capabilities of the provided [CoroutineContext].
 * We ensure they start in sequence so it's guaranteed to finish on a single threaded context.
 *
 * @see raceN for a function that ensures it runs in parallel on the [ComputationPool].
 */
suspend fun <A, B, C, D> raceN(
  ctx: CoroutineContext,
  fa: suspend () -> A,
  fb: suspend () -> B,
  fc: suspend () -> C,
  fd: suspend () -> D
): Race4<A, B, C, D> {
  fun onSuccess(
    isActive: AtomicBooleanW,
    main: SuspendConnection,
    other2: SuspendConnection,
    other3: SuspendConnection,
    other4: SuspendConnection,
    cb: (Result<Race4<A, B, C, D>>) -> Unit,
    r: Race4<A, B, C, D>
  ): Unit = if (isActive.getAndSet(false)) {
    other2.cancelToken().cancel.startCoroutine(Continuation(EmptyCoroutineContext) { r2 ->
      other3.cancelToken().cancel.startCoroutine(Continuation(EmptyCoroutineContext) { r3 ->
        other4.cancelToken().cancel.startCoroutine(Continuation(EmptyCoroutineContext) { r4 ->
          main.pop()
          r2.fold({
            r3.fold({
              r4.fold({ cb(Result.success(r)) }, { e -> cb(Result.failure(e)) })
            }, { e ->
              r4.fold({ cb(Result.failure(e)) }, { e2 -> cb(Result.failure(Platform.composeErrors(e, e2))) })
            })
          }, { e ->
            r3.fold({ cb(Result.failure(e)) }, { e2 -> cb(Result.failure(Platform.composeErrors(e, e2))) })
          })
        })
      })
    })
  } else Unit

  fun onError(
    active: AtomicBooleanW,
    cb: (Result<Nothing>) -> Unit,
    main: SuspendConnection,
    other2: SuspendConnection,
    other3: SuspendConnection,
    other4: SuspendConnection,
    err: Throwable
  ): Unit = if (active.getAndSet(false)) {
    other2.cancelToken().cancel.startCoroutine(Continuation(EmptyCoroutineContext) { r2 ->
      other3.cancelToken().cancel.startCoroutine(Continuation(EmptyCoroutineContext) { r3 ->
        other4.cancelToken().cancel.startCoroutine(Continuation(EmptyCoroutineContext) { r4 ->
          main.pop()
          cb(
            Result.failure(
              r2.fold({
                r3.fold({
                  r4.fold({
                    err
                  }, { err4 ->
                    Platform.composeErrors(err, err4)
                  })
                }, { err3 ->
                  r3.fold({
                    Platform.composeErrors(err, err3)
                  }, { err4 ->
                    Platform.composeErrors(err, err3, err4)
                  })
                })
              }, { err2 ->
                r3.fold({
                  Platform.composeErrors(err, err2)
                }, { err3 ->
                  Platform.composeErrors(err, err2, err3)
                })
              })
            )
          )
        })
      })
    })
  } else Unit

  return suspendCoroutineUninterceptedOrReturn { cont ->
    val conn = cont.context.connection()
    val cont = cont.intercepted()

    val active = AtomicBooleanW(true)
    val connA = SuspendConnection()
    val connB = SuspendConnection()
    val connC = SuspendConnection()
    val connD = SuspendConnection()

    conn.push(listOf(connA.cancelToken(), connB.cancelToken(), connC.cancelToken()))

    fa.startCoroutineCancellable(CancellableContinuation(ctx, connA) { result ->
      result.fold({
        onSuccess(active, conn, connB, connC, connD, cont::resumeWith, Race4.First(it))
      }, {
        onError(active, cont::resumeWith, conn, connB, connC, connD, it)
      })
    })

    fb.startCoroutineCancellable(CancellableContinuation(ctx, connB) { result ->
      result.fold({
        onSuccess(active, conn, connA, connC, connD, cont::resumeWith, Race4.Second(it))
      }, {
        onError(active, cont::resumeWith, conn, connA, connC, connD, it)
      })
    })

    fc.startCoroutineCancellable(CancellableContinuation(ctx, connC) { result ->
      result.fold({
        onSuccess(active, conn, connA, connB, connD, cont::resumeWith, Race4.Third(it))
      }, {
        onError(active, cont::resumeWith, conn, connA, connB, connD, it)
      })
    })

    fd.startCoroutineCancellable(CancellableContinuation(ctx, connD) { result ->
      result.fold({
        onSuccess(active, conn, connA, connB, connC, cont::resumeWith, Race4.Fourth(it))
      }, {
        onError(active, cont::resumeWith, conn, connA, connB, connC, it)
      })
    })

    COROUTINE_SUSPENDED
  }
}
