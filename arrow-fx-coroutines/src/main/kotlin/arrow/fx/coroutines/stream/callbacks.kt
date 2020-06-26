package arrow.fx.coroutines.stream

import arrow.core.Either
import arrow.fx.coroutines.CancelToken
import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.UnsafePromise
import arrow.fx.coroutines.andThen
import arrow.fx.coroutines.stream.concurrent.Queue
import kotlin.coroutines.Continuation
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.startCoroutine
import kotlin.experimental.ExperimentalTypeInference

interface EmitterSyntax<A> {
  fun emit(a: A): Unit
  fun emit(chunk: Chunk<A>): Unit
  fun emit(iterable: Iterable<A>): Unit
  fun emit(vararg aas: A): Unit
  fun end(): Unit
}

/**
 * Creates a Stream from the given suspended block.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.coroutines.stream.*
 *
 * //sampleStart
 * suspend fun main(): Unit =
 *   Stream.callbackStream {
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
 * Note that if neither `end()`, `emit(Chunk.empty())` nor other limit operators such as `take(N)` are called,
 * then the Stream will never end.
 */
//@OptIn(ExperimentalTypeInference::class) in 1.3.70
@UseExperimental(ExperimentalTypeInference::class)
fun <A> Stream.Companion.callbackStream(@BuilderInference f: suspend EmitterSyntax<A>.() -> Unit): Stream<A> =
  Stream.cancellableCallbackStream(f.andThen { CancelToken.unit })



/**
 * Creates a Stream from the given suspended block that will evaluate the passed CancelToken if cancelled.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.coroutines.stream.*
 *
 * //sampleStart
 * suspend fun main(): Unit =
 *   Stream.callbackStream {
 *       emit(1)
 *       emit(2, 3, 4)
 *       end()
 *       CancelToken { /* cancel subscription to callback */ }
 *     }
 *     .compile()
 *     .toList()
 *     .let(::println) //[1, 2, 3, 4]
 * //sampleEnd
 * ```
 *
 * Note that if neither `end()`, `emit(Chunk.empty())` nor other limit operators such as `take(N)` are called,
 * then the Stream will never end.
 */
//@OptIn(ExperimentalTypeInference::class) in 1.3.70
@UseExperimental(ExperimentalTypeInference::class)
fun <A> Stream.Companion.cancellableCallbackStream(@BuilderInference f: suspend EmitterSyntax<A>.() -> CancelToken): Stream<A> =
  effect {
    val q = Queue.unbounded<Chunk<A>>()
    val error = UnsafePromise<Throwable>()

    val cancel = emitterCallback(f) { value ->
      suspend {
        q.enqueue1(value)
        //TODO shall we consider emitting from different contexts? Might serve as an observeOn in RxJava
      }.startCoroutine(Continuation(EmptyCoroutineContext) { r -> r.fold({ Unit }, { e -> error.complete(Result.success(e)) })  })
    }

    q.dequeue()
      .interruptWhen { Either.Left(error.join()) }
      .terminateOn { it == Chunk.empty<A>() }
      .flatMap(::chunk)
      .onFinalizeCase {
        when (it) {
          is ExitCase.Cancelled -> cancel.cancel.invoke()
        }
      }
  }.flatten()

private suspend fun <A> emitterCallback(
  f: suspend EmitterSyntax<A>.() -> CancelToken,
  cb: (Chunk<A>) -> Unit
): CancelToken =
  object : EmitterSyntax<A> {
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
      cb(Chunk.empty())
    }
  }.f()
