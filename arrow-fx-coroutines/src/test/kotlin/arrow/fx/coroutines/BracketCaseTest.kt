package arrow.fx.coroutines

import arrow.core.Either
import io.kotest.assertions.fail
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.long
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

@ExperimentalTime
class BracketCaseTest : ArrowFxSpec(spec = {

//  "Uncancellable back pressures timeoutOrNull" {
//    checkAll(Arb.long(50, 100), Arb.long(300, 400)) { a, b ->
//      val (n, duration) = measureTimedValue {
//        withTimeoutOrNull(a) {
//          uncancellable { delay(b) }
//        }
//      }
//
//      n shouldBe null // timed-out so should be null
//      require((duration.inMilliseconds) >= b) {
//        "Should've taken longer than $b milliseconds, but took $duration"
//      }
//    }
//  }

  "Immediate acquire bracketCase finishes successfully" {
    checkAll(Arb.int(), Arb.int()) { a, b ->
      bracketCase(
        acquire = { a },
        use = { aa -> Pair(aa, b) },
        release = { _, _ -> CancelToken.unit }
      ) shouldBe Pair(a, b)
    }
  }

  "Suspended acquire bracketCase finishes successfully" {
    checkAll(Arb.int(), Arb.int()) { a, b ->
      bracketCase(
        acquire = { a.suspend() },
        use = { aa -> Pair(aa, b) },
        release = { _, _ -> CancelToken.unit }
      ) shouldBe Pair(a, b)
    }
  }

  "Immediate error in acquire stays the same error" {
    checkAll(Arb.throwable()) { e ->
      Either.catch {
        bracketCase<Unit, Int>(
          acquire = { throw e },
          use = { 5 },
          release = { _, _ -> CancelToken.unit }
        )
      } should leftException(e)
    }
  }

  "Suspend error in acquire stays the same error" {
    checkAll(Arb.throwable()) { e ->
      Either.catch {
        bracketCase<Unit, Int>(
          acquire = { e.suspend() },
          use = { 5 },
          release = { _, _ -> CancelToken.unit }
        )
      } should leftException(e)
    }
  }

  "Immediate use bracketCase finishes successfully" {
    checkAll(Arb.int(), Arb.int()) { a, b ->
      bracketCase(
        acquire = { a },
        use = { aa -> Pair(aa, b).suspend() },
        release = { _, _ -> CancelToken.unit }
      ) shouldBe Pair(a, b)
    }
  }

  "Suspended use bracketCase finishes successfully" {
    checkAll(Arb.int(), Arb.int()) { a, b ->
      bracketCase(
        acquire = { a },
        use = { aa -> Pair(aa, b).suspend() },
        release = { _, _ -> CancelToken.unit }
      ) shouldBe Pair(a, b)
    }
  }

  "bracketCase must run release task on use immediate error" {
    checkAll(Arb.int(), Arb.throwable()) { i, e ->
      val promise = CompletableDeferred<ExitCase>()

      Either.catch {
        bracketCase<Int, Int>(
          acquire = { i },
          use = { throw e },
          release = { _, ex ->
            if (!promise.complete(ex)) fail("Release should only be called once, called again with $ex")
          }
        )
      }

      promise.await() shouldBe ExitCase.Failure(e)
    }
  }

  "bracketCase must run release task on use suspended error" {
    checkAll(Arb.int(), Arb.throwable()) { x, e ->
      val promise = CompletableDeferred<Pair<Int, ExitCase>>()

      Either.catch {
        bracketCase<Int, Int>(
          acquire = { x },
          use = { e.suspend() },
          release = { xx, ex ->
            if (!promise.complete(Pair(xx, ex))) fail("Release should only be called once, called again with $ex")
          }
        )
      }

      promise.await() shouldBe Pair(x, ExitCase.Failure(e))
    }
  }

  "bracketCase must always run immediate release" {
    checkAll(Arb.int()) { x ->
      val promise = CompletableDeferred<Pair<Int, ExitCase>>()

      bracketCase(
        acquire = { x },
        use = { it },
        release = { xx, ex ->
          if (!promise.complete(Pair(xx, ex))) fail("Release should only be called once, called again with $ex")
        }
      )

      promise.await() shouldBe Pair(x, ExitCase.Completed)
    }
  }

  "bracketCase must always run suspended release" {
    checkAll(Arb.int()) { x ->
      val promise = CompletableDeferred<Pair<Int, ExitCase>>()

      bracketCase(
        acquire = { x },
        use = { it },
        release = { xx, ex ->
          if (!promise.complete(Pair(xx, ex))) fail("Release should only be called once, called again with $ex")
        })

      promise.await() shouldBe Pair(x, ExitCase.Completed)
    }
  }

  "bracketCase must always run immediate release error" {
    checkAll(Arb.int(), Arb.throwable()) { n, e ->
      Either.catch {
        bracketCase(
          acquire = { n },
          use = { it },
          release = { _, _ -> throw e }
        )
      } should leftException(e)
    }
  }

  "bracketCase must always run suspended release error" {
    checkAll(Arb.int(), Arb.throwable()) { n, e ->
      Either.catch {
        bracketCase(
          acquire = { n },
          use = { it },
          release = { _, _ -> e.suspend() }
        )
      } should leftException(e)
    }
  }

  "bracketCase must compose immediate use & immediate release error" {
    checkAll(Arb.int(), Arb.throwable(), Arb.throwable()) { n, e, e2 ->
      Either.catch {
        bracketCase<Int, Unit>(
          acquire = { n },
          use = { throw e },
          release = { _, _ -> throw e2 }
        )
      } shouldBe Either.Left(Platform.composeErrors(e, e2))
    }
  }

  "bracketCase must compose suspend use & immediate release error" {
    checkAll(Arb.int(), Arb.throwable(), Arb.throwable()) { n, e, e2 ->
      Either.catch {
        bracketCase<Int, Unit>(
          acquire = { n },
          use = { e.suspend() },
          release = { _, _ -> throw e2 }
        )
      } shouldBe Either.Left(Platform.composeErrors(e, e2))
    }
  }

  "bracketCase must compose immediate use & suspend release error" {
    checkAll(Arb.int(), Arb.throwable(), Arb.throwable()) { n, e, e2 ->
      Either.catch {
        bracketCase<Int, Unit>(
          acquire = { n },
          use = { throw e },
          release = { _, _ -> e2.suspend() }
        )
      } shouldBe Either.Left(Platform.composeErrors(e, e2))
    }
  }

  "bracketCase must compose suspend use & suspend release error" {
    checkAll(Arb.int(), Arb.throwable(), Arb.throwable()) { n, e, e2 ->
      Either.catch {
        bracketCase<Int, Unit>(
          acquire = { n },
          use = { e.suspend() },
          release = { _, _ -> e2.suspend() }
        )
      } shouldBe Either.Left(Platform.composeErrors(e, e2))
    }
  }

  "cancel on bracketCase releases with immediate acquire" {
    val start = CompletableDeferred<Unit>()
    val exit = CompletableDeferred<ExitCase>()

    val job = launch {
      bracketCase(
        acquire = { Unit },
        use = {
          // Signal that fiber is running
          start.complete(Unit)
          never<Unit>()
        },
        release = { _, exitCase ->
          if (!exit.complete(exitCase)) fail("Release should only be called once, called again with $exitCase")
        }
      )
    }

    // Wait until the fiber is started before cancelling
    start.await()
    job.cancelAndJoin()
    exit.await().shouldBeInstanceOf<ExitCase.Cancelled>()
  }

  "cancel on bracketCase releases with suspending acquire" {
    val start = CompletableDeferred<Unit>()
    val exit = CompletableDeferred<ExitCase>()

    val job = launch {
      bracketCase(
        acquire = { Unit.suspend() },
        use = {
          // Signal that fiber is running
          start.complete(Unit)
          never<Unit>()
        },
        release = { _, exitCase ->
          if (!exit.complete(exitCase)) fail("Release should only be called once, called again with $exitCase")
        }
      )
    }

    // Wait until the fiber is started before cancelling
    start.await()
    job.cancelAndJoin()
    exit.await().shouldBeInstanceOf<ExitCase.Cancelled>()
  }

  "cancel on bracketCase doesn't invoke after finishing" {
    val start = CompletableDeferred<Unit>()
    val exit = CompletableDeferred<ExitCase>()

    val job = launch {
      bracketCase(
        acquire = { Unit },
        use = { Unit.suspend() },
        release = { _, exitCase ->
          if (!exit.complete(exitCase)) fail("Release should only be called once, called again with $exitCase")
        }
      )

      // Signal that fiber can be cancelled running
      start.complete(Unit)
      never<Unit>()
    }

    // Wait until the fiber is started before cancelling
    start.await()
    job.cancelAndJoin()
    exit.await() shouldBe ExitCase.Completed
  }

  "acquire on bracketCase is not cancellable" {
    checkAll(Arb.int(), Arb.int()) { x, y ->
      val mVar = Channel<Int>(1).also { it.send(x) }
      val latch = CompletableDeferred<Unit>()
      val p = CompletableDeferred<ExitCase>()

      val job = launch {
        bracketCase(
          acquire = {
            latch.complete(Unit)
            // This should be uncancellable, and suspends until capacity 1 is received
            mVar.send(y)
          },
          use = { never<Unit>() },
          release = { _, exitCase ->
            if (!p.complete(exitCase)) fail("Release should only be called once, called again with $exitCase")
          })
      }

      // Wait until acquire started
      latch.await()
      job.cancel()

      mVar.receive() shouldBe x
      // If acquire was cancelled this hangs since the buffer is empty
      mVar.receive() shouldBe y
      p.await().shouldBeInstanceOf<ExitCase.Cancelled>()
    }
  }

  "release on bracketCase is not cancellable" {
    checkAll(Arb.int(), Arb.int()) { x, y ->
      val mVar = Channel<Int>(1).also { it.send(x) }
      val latch = CompletableDeferred<Unit>()

      val job = launch {
        bracketCase(
          acquire = { latch.complete(Unit) },
          use = { never<Unit>() },
          release = { _, _ ->
            kotlin.coroutines.coroutineContext.ensureActive()
            mVar.send(y)
          }
        )
      }

      latch.await()
      job.cancel()

      mVar.receive() shouldBe x
      // If release was cancelled this hangs since the buffer is empty
      withTimeoutOrNull(10_000) { mVar.receive() } shouldBe y
    }
  }
})
