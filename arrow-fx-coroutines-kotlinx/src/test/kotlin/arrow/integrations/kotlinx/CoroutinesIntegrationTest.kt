package arrow.integrations.kotlinx

import arrow.fx.coroutines.*
import io.kotest.assertions.fail
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.map
import io.kotest.property.arbitrary.string
import io.kotest.property.checkAll
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.test.TestCoroutineDispatcher
import kotlinx.coroutines.test.TestCoroutineExceptionHandler
import kotlinx.coroutines.test.TestCoroutineScope

@ExperimentalCoroutinesApi
@ObsoleteCoroutinesApi
@Suppress("IMPLICIT_NOTHING_AS_TYPE_PARAMETER")
class CoroutinesIntegrationTest : StringSpec({

  fun Arb.Companion.throwable(): Arb<Throwable> =
    Arb.string().map(::RuntimeException)

  // --------------- suspendCancellable ---------------

  "suspendedCancellable should resume with correct result" {
    checkAll(Arb.int()) { i ->
      val ceh = TestCoroutineExceptionHandler()
      val scope = TestCoroutineScope(ceh + TestCoroutineDispatcher())

      scope.launch {
        suspendCancellable {
          val first = i + 1
          cancelBoundary()
          val second = first + 1
          cancelBoundary()
          val third = second + 1
          cancelBoundary()
          third
        } shouldBe i + 3
      }

      ceh.uncaughtExceptions.isEmpty()
    }
  }

  "suspendedCancellable exceptions are captured into CoroutineExceptionHandler" {
    checkAll(Arb.throwable()) { expected ->
      val ceh = TestCoroutineExceptionHandler()
      val scope = TestCoroutineScope(ceh + TestCoroutineDispatcher())

      scope.launch {
        suspendCancellable { throw expected }
      }

      val actual = ceh.uncaughtExceptions[0]
      // suspendCancellableCoroutine copy and re-throws the exception so we need to compare the type
      // see https://github.com/Kotlin/kotlinx.coroutines/blob/master/kotlinx-coroutines-core/jvm/src/internal/StackTraceRecovery.kt#L68
      actual::class shouldBe expected::class
    }
  }

  "suspendCancellable doesn't start if scope already cancelled" {
    checkAll(Arb.int()) { i ->
      val scope = TestCoroutineScope(Job() + TestCoroutineDispatcher())
      val ref = AtomicRefW<Int?>(i)
      scope.cancel()
      scope.launch {
        cancelBoundary()
        ref.value = null
      }

      ref.value shouldBe i
    }
  }

  "scope cancellation cancels suspendedCancellable" {
    checkAll(Arb.int()) { i ->
      val scope = TestCoroutineScope(Job() + TestCoroutineDispatcher())
      val latch = Promise<Unit>()
      val promise = Promise<Int>()

      scope.launch {
        suspendCancellable {
          cancellableF<Unit> { _ ->
            latch.complete(Unit)
            CancelToken { promise.complete(i) }
          }
        }
      }

      latch.get()
      scope.cancel()
      promise.get() shouldBe i
    }
  }

  "suspendCancellable can cancel forever suspending tasks" {
    val latch = Promise<Unit>()
    val promise = Promise<ExitCase>()
    val scope = TestCoroutineScope(Job() + TestCoroutineDispatcher())

    scope.launch {
      suspendCancellable {
        guaranteeCase({
          latch.complete(Unit)
          never<Unit>()
        }) { case -> promise.complete(case) }
      }
    }

    latch.get()
    scope.cancel()
    promise.get() shouldBe ExitCase.Cancelled
  }

  // --------------- unsafeRunScoped ---------------

  "unsafeRunScoped captures exception and returns Result.failure" {
    checkAll(Arb.throwable()) { e ->
      val scope = TestCoroutineScope(TestCoroutineDispatcher())
      val promise = CompletableDeferred<Result<Int>>()

      scope.unsafeRunScoped({ throw e }) {
        promise.complete(it)
      }

      promise.await() shouldBe Result.failure(e)
    }
  }

  "unsafeRunScoped should cancel correctly" {
    checkAll(Arb.int()) { i ->
      val scope = TestCoroutineScope(Job() + TestCoroutineDispatcher())
      val promise = Promise<Int>()

      suspend fun cancellable(): Unit =
        cancellable<Unit> { _ ->
          CancelToken { promise.complete(i) }
        }

      scope.unsafeRunScoped({ cancellable() }) { }

      scope.cancel()
      promise.get()
    }
  }

  "unsafeRunScoped can cancel forever suspending tasks" {
    val scope = TestCoroutineScope(Job() + TestCoroutineDispatcher())
    val latch = Promise<Unit>()
    val promise = Promise<ExitCase>()

    scope.unsafeRunScoped({
      guaranteeCase({
        latch.complete(Unit)
        never<Unit>()
      }) { case -> promise.complete(case) }
    }) { }

    latch.get()
    scope.cancel()
    promise.get()
  }

  "should complete when running a pure value with unsafeRunScoped" {
    checkAll(Arb.int()) { i ->
      val scope = TestCoroutineScope(TestCoroutineDispatcher())
      val promise = CompletableDeferred<Int>()
      scope.unsafeRunScoped(
        { i },
        { it.fold({ ii -> promise.complete(ii) }, { fail("") }) }
      )
      promise.await() shouldBe i
    }
  }

  "unsafeRunScoped doesn't start if scope is cancelled" {
    checkAll(Arb.int()) { i ->
      val scope = TestCoroutineScope(Job() + TestCoroutineDispatcher())
      val ref = AtomicRefW<Int?>(i)
      scope.cancel()
      scope.unsafeRunScoped({ ref.value = null }) {}
      ref.value shouldBe i
    }
  }

  "unsafeRunScoped rethrows exception from callback" {
    checkAll(Arb.throwable()) { e ->
      val scope = TestCoroutineScope(TestCoroutineDispatcher())

      shouldThrow<Throwable> {
        scope.unsafeRunScoped({ throw e }) {
          it.fold({ fail("Excepted $e but found $it") }, { throw it })
        }
      } shouldBe e
    }
  }

  // --------------- forkScoped ---------------

  "ForkScoped can cancel forever suspending tasks" {
    checkAll(Arb.int()) { i ->
      val scope = TestCoroutineScope(Job() + TestCoroutineDispatcher())
      val latch = Promise<Unit>()
      val promise = Promise<ExitCase>()

      ForkScoped(scope) {
        guaranteeCase({
          latch.complete(Unit)
          never<Unit>()
        }) { case -> promise.complete(case) }
      }

      latch.get()
      scope.cancel()
      promise.get() shouldBe i
    }
  }

  "ForkScoped should cancel correctly" {
    checkAll(Arb.int()) { i ->
      val scope = TestCoroutineScope(Job() + TestCoroutineDispatcher())
      val promise = Promise<Int>()

      suspend fun cancellable(): Unit =
        cancellable<Unit> { _ ->
          CancelToken { promise.complete(i) }
        }

      ForkScoped(scope) { cancellable() }

      scope.cancel()
      promise.get() shouldBe i
    }
  }

  "ForkScoped should complete when running a pure value" {
    checkAll(Arb.int()) { i ->
      val scope = TestCoroutineScope(Job() + TestCoroutineDispatcher())
      val f = ForkScoped(scope) { i }
      f.join() shouldBe i
    }
  }


  "ForkScoped doesn't start if scope is cancelled" {
    checkAll(Arb.int()) { i ->
      val scope = TestCoroutineScope(Job() + TestCoroutineDispatcher())
      val ref = AtomicRefW<Int?>(i)
      scope.cancel()

      ForkScoped(scope) {
        ref.value = null
      }

      ref.value shouldBe i
    }
  }
})
