package arrow.fx.coroutines.stream

import arrow.core.Either
import arrow.fx.coroutines.CancelToken
import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.ForkConnected
import arrow.fx.coroutines.UnsafePromise
import arrow.fx.coroutines.guaranteeCase
import arrow.fx.coroutines.stream.concurrent.Queue
import kotlin.coroutines.Continuation
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.startCoroutine
import kotlin.experimental.ExperimentalTypeInference

private object END

interface EmitterSyntax<A> {
  fun onCancel(cancelF: suspend () -> Unit): Unit
  fun emit(a: A): Unit
  fun emit(chunk: Chunk<A>): Unit
  fun emit(iterable: Iterable<A>): Unit
  fun emit(vararg aas: A): Unit
  fun end(): Unit
}

/**
 * Creates a Stream from the given suspended block, allowing to emit, set cancel effects and end the emission.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.coroutines.stream.*
 * import arrow.fx.coroutines.CancelToken
 *
 * //sampleStart
 * suspend fun main(): Unit =
 *   Stream.cancellable {
 *       onCancel { /* cancel something */ }
 *       emit(1)
 *       emit(2, 3, 4)
 *       end()
 *     }
 *     .compile()
 *     .toList()
 *     .let(::println) //[1, 2, 3, 4]
 * //sampleEnd
 * ```
 *
 * Note that if neither `end()` nor other limit operators such as `take(N)` are called,
 * then the Stream will never end.
 *
 * Cancellation or errors might happen any time during emission, so it's recommended to call `onCancel` as early as
 * possible, otherwise it's not guaranteed to be called.
 */
// @OptIn(ExperimentalTypeInference::class) in 1.3.70
@UseExperimental(ExperimentalTypeInference::class)
fun <A> Stream.Companion.async(@BuilderInference f: suspend EmitterSyntax<A>.() -> Unit): Stream<A> =
  force {
    val q = Queue.unbounded<Any?>()
    val error = UnsafePromise<Throwable>()
    val cancel = UnsafePromise<CancelToken>()

    ForkConnected { emitterCallback(f, cancel, error, q) }

    (q.dequeue()
      .interruptWhen { Either.Left(error.join()) }
      .terminateOn { it === END } as Stream<Chunk<A>>)
      .flatMap(::chunk)
      .onFinalizeCase {
        when (it) {
          is ExitCase.Cancelled -> {
            when (val r = cancel.tryGet()) {
              null -> Unit // This means the user didn't set the onCancel or set it too late
              else -> r.getOrNull()!!.invoke()
            }
          }
        }
      }
  }

private suspend fun <A> emitterCallback(
  f: suspend EmitterSyntax<A>.() -> Unit,
  cancel: UnsafePromise<CancelToken>,
  error: UnsafePromise<Throwable>,
  q: Queue<Any?>
): Unit {
  val cb = { ch: Any? ->
    suspend {
      q.enqueue1(ch)
    }.startCoroutine(Continuation(EmptyCoroutineContext) { r ->
      r.fold({ Unit }, { e -> error.complete(Result.success(e)) })
    })
  }

  val emitter = object : EmitterSyntax<A> {
    override fun emit(a: A) {
      emit(Chunk.just(a))
    }

    override fun emit(chunk: Chunk<A>) {
      cb(chunk)
    }

    override fun emit(iterable: Iterable<A>) {
      cb(Chunk.iterable(iterable))
    }

    override fun emit(vararg aas: A) {
      cb(Chunk(*aas))
    }

    override fun end() {
      cb(END)
    }

    override fun onCancel(cancelF: suspend () -> Unit) {
      cancel.complete(Result.success(CancelToken(cancelF)))
    }
  }

  guaranteeCase({ emitter.f() }, { exit ->
    when (exit) {
      is ExitCase.Failure -> error.complete(Result.success(exit.failure))
    }
  })
}
