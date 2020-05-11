package arrow.fx

import arrow.core.Either
import me.eugeniomarletti.kotlin.metadata.shadow.utils.addToStdlib.safeAs
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.CompletionStage
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future

fun <A> CompletionStage<A>.toIo(): IO<Throwable, A> =
  safeAs<Future<A>>()?.let { future ->
    future.toIoNow() ?: IO.cancellable { onReady ->
      whenCompleteAsync(onReady)
      IO { future.cancel(false); Unit }
    }
  } ?: IO.async { onReady -> whenCompleteAsync(onReady) }

fun <A> Future<A>.toIoNow(): IO<Throwable, A>? = when {
  isCancelled -> IO.never
  isDone -> runCatching { get() }.fold({ IO.just(it) }, { e -> e.sanitize().raiseOrForget() })
  else -> null
}

private inline fun <A> CompletionStage<A>.whenCompleteAsync(
  crossinline onReady: (IOResult<Throwable, A>) -> Unit
): CompletionStage<A> = whenCompleteAsync { a, e ->
  completion(e, a)?.toIoResult()?.let(onReady)
}

private fun Throwable?.raiseOrForget(): IO<Throwable, Nothing> =
  this
    ?.let { IO.raiseError<Throwable, Nothing>(it) }
    ?: IO.never

// TODO(pabs): Convert Either<Throwable, A>? to Wedge (future work) https://github.com/arrow-kt/arrow-core/issues/87
private fun <A> completion(error: Throwable?, success: A?): Either<Throwable, A>? = when {
  error != null -> error.sanitize()?.let { Either.Left(it) }
  success != null -> Either.Right(success)
  else -> Either.Left(CompletionStageCompleteWithoutResolution)
}

private fun <A> Either<Throwable, A>.toIoResult(): IOResult<Throwable, A> =
  fold({ e -> IOResult.Error(e) }, { a -> IOResult.Success(a) })

private fun Throwable.sanitize(): Throwable? = when (this) {
  is ExecutionException, is CompletionException -> safeCause
  is CancellationException -> null
  else -> this
}

private val Throwable.safeCause
  get() = when (val cause = cause) {
    null -> MissingCause(this)
    else -> cause
  }

/**
 * Thrown when there is no success, failure, or cancellation
 */
object CompletionStageCompleteWithoutResolution : Exception()

/**
 * Thrown when we get an exception without any cause
 */
data class MissingCause(val e: Throwable) : Exception()

fun <A> IO<Throwable, A>.toCompletableFuture(): CompletionStage<A> =
  CompletableFuture<A>().also { future ->
    runAsync { result ->
      when (result) {
        is IOResult.Success -> future.complete(result.value)
        is IOResult.Error -> future.completeExceptionally(result.error)
        is IOResult.Exception -> future.completeExceptionally(result.exception)
      }
      IO.unit
    }
  }
