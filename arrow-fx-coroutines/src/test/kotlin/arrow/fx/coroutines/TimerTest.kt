package arrow.fx.coroutines

import arrow.core.Either
import arrow.core.Right
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.positiveInts
import io.kotest.property.checkAll
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CompletionHandlerException
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.withContext
import kotlinx.coroutines.delay
import kotlin.time.milliseconds
import kotlin.time.minutes
import kotlin.time.nanoseconds

@OptIn(InternalCoroutinesApi::class)
class TimerTest : ArrowFxSpec(spec = {

  suspend fun timeNano(): Long =
    System.nanoTime()

  suspend fun timeMilis(): Long =
    System.currentTimeMillis()

  "delay returns on the original context" {
    single.use { ctx ->
      checkAll(Arb.int()) {
        withContext(ctx) {
          val n0 = threadName.invoke()
          delay(10.milliseconds)
          val n = threadName.invoke()

          n0 shouldBe n
        }
      }
    }
  }

  "timeOutOrNull" - {
    "returns to original context without timing out" {
      Resource.singleThreadContext("1").zip(single).use { (one, single) ->
        checkAll {
          withContext(single) {
            val n0 = threadName.invoke()

            timeOutOrNull(1.minutes) {
              one.shift()
              threadName.invoke() shouldBe "1"
              Unit.suspend()
            }

            n0 shouldBe threadName.invoke()
          }
        }
      }
    }

    "returns to original context when timing out" {
      Resource.singleThreadContext("1").zip(single).use { (one, single) ->
        checkAll {
          withContext(single) {
            val n0 = threadName.invoke()

            timeOutOrNull(50.milliseconds) {
              one.shift()
              threadName.invoke() shouldBe "1"
              never<Unit>()
            } shouldBe null

            n0 shouldBe threadName.invoke()
          }
        }
      }
    }

    "returns to original context on failure" {
      Resource.singleThreadContext("1").zip(single).use { (one, single) ->
        checkAll(Arb.throwable()) { e ->
          withContext(single) {
            val n0 = threadName.invoke()

            Either.catch {
              timeOutOrNull(50.milliseconds) {
                one.shift()
                threadName.invoke() shouldBe "1"
                e.suspend()
              }
            } should leftException(e)

            n0 shouldBe threadName.invoke()
          }
        }
      }
    }

    "returns to original context on CancelToken failure" {
      Resource.singleThreadContext("1").zip(single).use { (one, single) ->
        checkAll(Arb.throwable()) { e ->
          val promised = CompletableDeferred<Throwable>()
          val handler = CoroutineExceptionHandler { _, e ->
            promised.complete(e)
          }
          withContext(single + handler) {
            val n0 = threadName.invoke()

            Either.catch {
              timeOutOrNull(50.milliseconds) {
                cancellableF<Nothing> {
                  one.shift()
                  threadName.invoke() shouldBe "1"

                  // This exception is send to CoroutineExceptionHandler
                  CancelToken { e.suspend() }
                }
              }
            } shouldBe Right(null)

            n0 shouldBe threadName.invoke()
          }
          promised.await().shouldBeInstanceOf<CompletionHandlerException> {
            it.cause shouldBe e
          }
        }
      }
    }
  }

  "sleep should last specified time" {
    checkAll(Arb.positiveInts(max = 50)) { i ->
      val length = i.toLong()
      val start = timeNano()
      delay(length.milliseconds)
      val end = timeNano()
      require((end - start) >= length) { "Expected (end - start) >= length but found ($end - $start) <= $length" }
    }
  }

  "negative delay should be immediate" {
    checkAll(Arb.int(Int.MIN_VALUE, -1)) { i ->
      val start = timeNano()
      delay(i.nanoseconds)
      val end = timeNano()
      require((start - end) <= 0L) { "Expected (end - start) <= 0L but found (${start - end}) <= 0L" }
    }
  }

  "delay can be cancelled" {
    checkAll(Arb.int(100, 500)) { d ->
      assertCancellable { delay(d.milliseconds) }
    }
  }

  "timeout can lose" {
    checkAll(Arb.int()) { i ->
      timeOutOrNull(1.milliseconds) {
        delay(100.milliseconds)
        i
      } shouldBe null
    }
  }

  "timeout wins non suspend" {
    checkAll(Arb.int()) { i ->
      timeOutOrNull(10.milliseconds) {
        i
      } shouldBe i
    }
  }

  "timeout wins suspend" {
    checkAll(Arb.int()) { i ->
      timeOutOrNull(100.milliseconds) {
        i.suspend()
      } shouldBe i
    }
  }

  "time-out cancels the token" {
    checkAll(Arb.int()) { i ->
      val promise = Promise<Int>()

      timeOutOrNull(50.milliseconds) {
        cancellable<Nothing> {
          CancelToken { promise.complete(i) }
        }
      } shouldBe null

      promise.get() shouldBe i
    }
  }
})
