package arrow.fx

import arrow.core.Either
import arrow.core.None
import arrow.core.test.UnitSpec
import arrow.fx.typeclasses.seconds
import io.kotlintest.matchers.boolean.shouldBeTrue
import io.kotlintest.shouldBe
import io.mockk.Call
import io.mockk.MockKAnswerScope
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.CompletionStage
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.function.BiConsumer

class CompletableFutureIOSpecs : UnitSpec() {

  init {

    "CompletableFuture with success -> on IO.Right" {
      val future = CompletableFuture.completedFuture("Great success!")

      val io = future.toIo()

      io.unsafeRunSyncEither() shouldBe Either.Right("Great success!")
    }

    "CompletableFuture with delayed success -> IO.Right" {
      val future = CompletableFuture<String>()

      val io = future.toIo()
      future.complete("Great success!")

      io.unsafeRunSyncEither() shouldBe Either.Right("Great success!")
    }

    "CompletableFuture with error -> on IO.Left" {
      val future = CompletableFuture.supplyAsync<String> { throw ExpectedError }

      val io = future.toIo()

      io.unsafeRunSyncEither() shouldBe Either.Left(ExpectedError)
    }

    "CompletableFuture with delayed error -> IO.Left" {
      val future = CompletableFuture<String>()
      val io = future.toIo()

      future.completeExceptionally(ExpectedError)

      io.unsafeRunSyncEither() shouldBe Either.Left(ExpectedError)
    }

    "CompletableFuture with cancellation -> nothing, ever" {
      val future = CompletableFuture<String>()
      future.cancel(true)
      future.complete("should not see this")

      val io = future.toIo()

      io.unsafeRunTimed(2.seconds) shouldBe None
    }

    "CompletableFuture with delayed cancellation -> nothing, ever" {
      val future = CompletableFuture<String>()
      val io = future.toIo()

      future.cancel(true)
      future.complete("should not see this")

      io.unsafeRunTimed(2.seconds) shouldBe None
    }

    "IO cancelled -> CompletableFuture is cancelled" {
      val future = CompletableFuture<String>()
      val io = future.toIo()

      val cancel = io.unsafeRunAsyncCancellableEither(OnCancel.Silent) {}
      cancel()

      future.isCancelled.shouldBeTrue()
    }

    "CompletionStage with success -> IO.Right" {
      val stage = fakeCompletionStage(a = "Great success!")

      val io = stage.toIo()

      io.unsafeRunSyncEither() shouldBe Either.Right("Great success!")
    }

    "CompletionStage with error -> IO.Left" {
      val stage = fakeCompletionStage<String>(e = ExpectedError)

      val io = stage.toIo()

      io.unsafeRunSyncEither() shouldBe Either.Left(ExpectedError)
    }

    "CompletionStage with no resolution -> IO.Left without resolution" {
      val stage = fakeCompletionStage<String>()

      val io = stage.toIo()

      io.unsafeRunSyncEither() shouldBe Either.Left(CompletionStageCompleteWithoutResolution)
    }

    "CancellationException is ignored" {
      val stage = fakeCompletionStage<String>(e = CancellationException())

      val io = stage.toIo()

      io.unsafeRunTimed(2.seconds) shouldBe None
    }

    "ExecutionException without a cause -> IO.Left<MissingCause>" {
      val exception = ExecutionException(null)
      val stage = fakeCompletionStage<String>(e = exception)

      val io = stage.toIo()

      io.unsafeRunSyncEither() shouldBe Either.Left(MissingCause(exception))
    }

    "CompletionException without a cause -> IO.Left<MissingCause>" {
      val exception = CompletionException(null)
      val stage = fakeCompletionStage<String>(e = exception)

      val io = stage.toIo()

      io.unsafeRunSyncEither() shouldBe Either.Left(MissingCause(exception))
    }

    "Future is done with success -> toIoNow() -> IO.Left" {
      val future = fakeFuture<String>(isDone = { "Great Success!" })

      val io = future.toIoNow()

      io?.unsafeRunSyncEither() shouldBe Either.Right("Great Success!")
    }

    "Future is done with error -> toIoNow() -> IO.Left" {
      val future = fakeFuture<String>(isDone = { throw ExpectedError })

      val io = future.toIoNow()

      io?.unsafeRunSyncEither() shouldBe Either.Left(ExpectedError)
    }

    "Future is cancelled -> toIoNow() -> nothing, ever" {
      val future = fakeFuture<String>(isCancelled = true)

      val io = future.toIoNow()

      io?.unsafeRunTimed(2.seconds) shouldBe None
      verify(exactly = 0) { future.get() }
    }

    "Future is pending -> toIoNow() -> null" {
      val future = fakeFuture<String>()

      val io = future.toIoNow()

      io shouldBe null
    }
  }
}

private object ExpectedError : Throwable()

private fun <A> fakeFuture(
  isCancelled: Boolean = false,
  isDone: (MockKAnswerScope<A, A>.(Call) -> A)? = null
) = mockk<Future<A>> {
  every { this@mockk.isCancelled } returns isCancelled
  every { this@mockk.isDone } returns (isDone != null)
  isDone?.also { answer ->
    every { get() } answers answer
  }
}

@Suppress("UNCHECKED_CAST") // to return self
private fun <A> fakeCompletionStage(a: A? = null, e: Throwable? = null) =
  mockk<CompletionStage<A>> {
    every { whenCompleteAsync(any()) } answers {
      arg<BiConsumer<A?, Throwable?>>(0).accept(a, e)
      self as CompletionStage<A>
    }
  }
