package arrow.fx

import java.util.concurrent.CompletableFuture

fun <A> CompletableFuture<A>.toIo(): IO<Throwable, A> =
  IO.cancellable { cb ->
    try {
      cb(IOResult.Success(get()))
    } catch (e: Throwable) {
      cb(IOResult.Exception(e))
    }
    IO<Unit> { cancel(true) }
  }

fun <A> IO<Throwable, A>.toCompletableFuture(): CompletableFuture<A> =
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
