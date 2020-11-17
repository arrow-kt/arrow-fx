package arrow.fx.coroutines

import arrow.Kind
import arrow.continuations.generic.DelimitedContinuation
import arrow.continuations.generic.DelimitedScope
import arrow.continuations.generic.MultiShotDelimContScope
import arrow.continuations.generic.NestedDelimContScope
import arrow.core.Either
import arrow.core.EitherPartialOf
import arrow.core.Right
import arrow.core.fix
import arrow.typeclasses.suspended.BindSyntax
import io.kotest.assertions.fail
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.loop
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.startCoroutineUninterceptedOrReturn
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

class EvalOnTests : ArrowFxSpec(spec = {

  "immediate value" {
    checkAll(Arb.int()) { i ->
      evalOn(ComputationPool) {
        i
      } shouldBe i
    }
  }

  "suspend value" {
    checkAll(Arb.int()) { i ->
      evalOn(ComputationPool) {
        i.suspend()
      } shouldBe i
    }
  }

  "evalOn on the same context doesn't dispatch" {
    suspend fun onSameContext(): String =
      evalOn(ComputationPool) {
        Thread.currentThread().name
      }

    checkAll {
      Platform.unsafeRunSync(ComputationPool) {
        val startOn = Thread.currentThread().name
        onSameContext() shouldBe startOn
        Thread.currentThread().name == startOn
      }
    }
  }

  "evalOn on the same context doesn't intercept" {
    suspend fun onComputation(ctx: CoroutineContext): String =
      evalOn(ctx) {
        Thread.currentThread().name
      }

    checkAll {
      val interceptor = TestableContinuationInterceptor()

      Platform.unsafeRunSync(interceptor) {
        val startOn = Thread.currentThread().name
        onComputation(interceptor) shouldBe startOn
        Thread.currentThread().name shouldBe startOn
        interceptor.timesIntercepted() shouldBe 0
      }
    }
  }

  "evalOn on a different context with the same ContinuationInterceptor doesn't intercept" {
    suspend fun onComputation(ctx: CoroutineContext): String =
      evalOn(ctx + CoroutineName("Different coroutine")) {
        Thread.currentThread().name
      }

    checkAll {
      val interceptor = TestableContinuationInterceptor()

      Platform.unsafeRunSync(interceptor) {
        val startOn = Thread.currentThread().name
        onComputation(interceptor) shouldBe startOn
        Thread.currentThread().name shouldBe startOn
        interceptor.timesIntercepted() shouldBe 0
      }
    }
  }

  "evalOn on a different context with a different ContinuationInterceptor does intercept" {
    suspend fun onComputation(): String =
      evalOn(IOPool) {
        Thread.currentThread().name
      }

    checkAll { // Run this test on single thread context to guarantee name
      single.use { ctx ->
        Platform.unsafeRunSync(ctx) {
          val startOn = Thread.currentThread().name
          onComputation() shouldNotBe startOn
          Thread.currentThread().name shouldBe startOn
        }
      }
    }
  }

  "immediate exception on KotlinX Dispatchers" {
    checkAll(Arb.int(), Arb.throwable()) { i, e ->
      val r = try {
        evalOn<Int>(coroutineContext) {
          throw e
        }
        fail("Should never reach this point")
      } catch (throwable: Throwable) {
        throwable shouldBe e
        i
      }

      r shouldBe i
    }
  }

  "suspend exception on KotlinX Dispatchers" {
    checkAll(Arb.int(), Arb.throwable()) { i, e ->
      val r = try {
        evalOn<Int>(coroutineContext) {
          e.suspend()
        }
        fail("Should never reach this point")
      } catch (throwable: Throwable) {
        throwable shouldBe e
        i
      }

      r shouldBe i
    }
  }

  "immediate exception from wrapped KotlinX Dispatcher" {
    checkAll(Arb.int(), Arb.throwable()) { i, e ->
      val r = try {
        evalOn<Int>(wrapperKotlinXDispatcher(coroutineContext)) {
          throw e
        }
        fail("Should never reach this point")
      } catch (throwable: Throwable) {
        throwable shouldBe e
        i
      }

      r shouldBe i
    }
  }

  "suspend exception from wrapped KotlinX Dispatcher" {
    checkAll(Arb.int(), Arb.throwable()) { i, e ->
      val r = try {
        evalOn<Int>(wrapperKotlinXDispatcher(coroutineContext)) {
          e.suspend()
        }
        fail("Should never reach this point")
      } catch (throwable: Throwable) {
        throwable shouldBe e
        i
      }

      r shouldBe i
    }
  }

  "immediate exception from Arrow Fx Pool" {
    checkAll(Arb.int(), Arb.throwable()) { i, e ->
      val r = try {
        evalOn<Int>(IOPool) {
          throw e
        }
        fail("Should never reach this point")
      } catch (throwable: Throwable) {
        throwable shouldBe e
        i
      }

      r shouldBe i
    }
  }

  "suspend exception from Arrow Fx Pool" {
    checkAll(Arb.int(), Arb.throwable()) { i, e ->
      val r = try {
        evalOn<Int>(IOPool) {
          e.suspend()
        }
        fail("Should never reach this point")
      } catch (throwable: Throwable) {
        throwable shouldBe e
        i
      }

      r shouldBe i
    }
  }

  "it just prints the different threads the cursor is in without MDCContext with Arrow's Environment" {
    single.use { ctx ->
      Environment(ctx).unsafeRunSync {
        Thread.currentThread().name.also(::println) shouldBe singleThreadName // single

        parTupledN(
          ComputationPool,
          { Thread.currentThread().name.also(::println) shouldNotBe singleThreadName }, // ForkJoinPool-1-worker-11
          { Thread.currentThread().name.also(::println) shouldNotBe singleThreadName } // ForkJoinPool-1-worker-9
        )

//        printContext("Before either")

        val result: Either<Throwable, String> = either {
          Thread.currentThread().name.also(::println) shouldBe singleThreadName // single

//          printContext("In either before evalOn")
          val threadName: String = evalOn(ComputationPool) {
//            printContext("In either in evalOn")
            Thread.currentThread().name/*.right()*/
          }/*.bind()*/

//          printContext("In either after evalOn")

          threadName.also(::println) shouldNotBe singleThreadName // ForkJoinPool-1-worker-9
          Thread.currentThread().name.also(::println) shouldBe singleThreadName // single

          parTupledN(
            ComputationPool,
            { Thread.currentThread().name.also(::println) shouldNotBe singleThreadName }, // ForkJoinPool-1-worker-9
            { Thread.currentThread().name.also(::println) shouldNotBe singleThreadName } // ForkJoinPool-1-worker-11
          )

          threadName.also(::println)
        }

        result shouldBe Right(singleThreadName)
      }
    }
  }
})

suspend fun printContext(prefer: String): Unit =
  suspendCoroutineUninterceptedOrReturn { cont ->
    println("$prefer ${cont.context}")
  }


private class TestableContinuationInterceptor : AbstractCoroutineContextElement(ContinuationInterceptor),
  ContinuationInterceptor {

  // Starting to run test always starts with an initial intercepted, so start count on -1.
  private val invocations = atomic(-1)

  companion object Key : CoroutineContext.Key<ContinuationInterceptor>

  override fun <T> interceptContinuation(continuation: Continuation<T>): Continuation<T> {
    invocations.incrementAndGet()
    return continuation
  }

  fun timesIntercepted(): Int = invocations.value
}

private fun wrapperKotlinXDispatcher(context: CoroutineContext): CoroutineContext {
  val dispatcher = context[ContinuationInterceptor] as CoroutineDispatcher
  return object : CoroutineDispatcher() {
    override fun isDispatchNeeded(context: CoroutineContext): Boolean =
      dispatcher.isDispatchNeeded(context)

    override fun dispatch(context: CoroutineContext, block: Runnable) =
      dispatcher.dispatch(context, block)
  }
}


suspend fun <E, A> either(c: suspend BindSyntax<EitherPartialOf<E>>.() -> A): Either<E, A> =
  suspendCoroutineUninterceptedOrReturn { cont ->
    println("DelimContScope.reset(${cont.context})")
    DelimContScope.reset(cont.context) {
      Either.Right(
        c(object : BindSyntax<EitherPartialOf<E>> {
          override suspend fun <A> Kind<EitherPartialOf<E>, A>.invoke(): A =
            when (val v = fix()) {
              is Either.Right -> v.b
              is Either.Left -> shift { v }
            }
        })
      )
    }
  }

/**
 * Implements delimited continuations with with no multi shot support (apart from shiftCPS which trivially supports it).
 *
 * For a version that simulates multishot (albeit with drawbacks) see [MultiShotDelimContScope].
 * For a version that allows nesting [reset] and calling parent scopes inside inner scopes see [NestedDelimContScope].
 *
 * The basic concept here is appending callbacks and polling for a result.
 * Every shift is evaluated until it either finishes (short-circuit) or suspends (called continuation). When it suspends its
 *  continuation is appended to a list waiting to be invoked with the final result of the block.
 * When running a function we jump back and forth between the main function and every function inside shift via their continuations.
 */
class DelimContScope<R>(val ctx: CoroutineContext = EmptyCoroutineContext, val f: suspend DelimitedScope<R>.() -> R) : DelimitedScope<R> {

  /**
   * Variable used for polling the result after suspension happened.
   */
  private val resultVar = atomic<Any?>(EMPTY_VALUE)

  /**
   * Variable for the next shift block to (partially) run, if it is empty that usually means we are done.
   */
  private val nextShift = atomic<(suspend () -> R)?>(null)

  /**
   * "Callbacks"/partially evaluated shift blocks which now wait for the final result
   */
  // TODO This can be append only, but needs fast reversed access
  private val shiftFnContinuations = mutableListOf<Continuation<R>>()

  /**
   * Small wrapper that handles invoking the correct continuations and appending continuations from shift blocks
   */
  data class SingleShotCont<A, R>(
    private val continuation: Continuation<A>,
    private val shiftFnContinuations: MutableList<Continuation<R>>
  ) : DelimitedContinuation<A, R> {
    override suspend fun invoke(a: A): R = suspendCoroutine { resumeShift ->
      shiftFnContinuations.add(resumeShift)
      continuation.resume(a)
    }
  }

  /**
   * Wrapper that handles invoking manually cps transformed continuations
   */
  data class CPSCont<A, R>(
    private val runFunc: suspend DelimitedScope<R>.(A) -> R
  ) : DelimitedContinuation<A, R> {
    override suspend fun invoke(a: A): R = DelimContScope<R> { runFunc(a) }.invoke()
  }

  /**
   * Captures the continuation and set [f] with the continuation to be executed next by the runloop.
   */
  override suspend fun <A> shift(f: suspend DelimitedScope<R>.(DelimitedContinuation<A, R>) -> R): A =
    suspendCoroutineUninterceptedOrReturn { continueMain ->
      val delCont = SingleShotCont(continueMain, shiftFnContinuations)
      assert(nextShift.compareAndSet(null, suspend { this.f(delCont) }))
      COROUTINE_SUSPENDED
    }

  /**
   * Same as [shift] except we never resume execution because we only continue in [c].
   */
  override suspend fun <A, B> shiftCPS(f: suspend (DelimitedContinuation<A, B>) -> R, c: suspend DelimitedScope<B>.(A) -> B): Nothing =
    suspendCoroutineUninterceptedOrReturn {
      assert(nextShift.compareAndSet(null, suspend { f(CPSCont(c)) }))
      COROUTINE_SUSPENDED
    }

  /**
   * Unsafe if [f] calls [shift] on this scope! Use [NestedDelimContScope] instead if this is a problem.
   */
  override suspend fun <A> reset(f: suspend DelimitedScope<A>.() -> A): A =
    DelimContScope(ctx, f).invoke()

  @Suppress("UNCHECKED_CAST")
  fun invoke(): R {
    f.startCoroutineUninterceptedOrReturn(this, Continuation(ctx) { result ->
      resultVar.value = result.getOrThrow()
    }).let {
      if (it == COROUTINE_SUSPENDED) {
        // we have a call to shift so we must start execution the blocks there
        resultVar.loop { mRes ->
          if (mRes === EMPTY_VALUE) {
            val nextShiftFn = nextShift.getAndSet(null)
              ?: throw IllegalStateException("No further work to do but also no result!")
            nextShiftFn.startCoroutineUninterceptedOrReturn(Continuation(ctx) { result ->
              resultVar.value = result.getOrThrow()
            }).let { nextRes ->
              // If we suspended here we can just continue to loop because we should now have a new function to run
              // If we did not suspend we short-circuited and are thus done with looping
              if (nextRes != COROUTINE_SUSPENDED) resultVar.value = nextRes as R
            }
            // Break out of the infinite loop if we have a result
          } else return@let
        }
      }
      // we can return directly if we never suspended/called shift
      else return@invoke it as R
    }
    assert(resultVar.value !== EMPTY_VALUE)
    // We need to finish the partially evaluated shift blocks by passing them our result.
    // This will update the result via the continuations that now finish up
    for (c in shiftFnContinuations.asReversed()) c.resume(resultVar.value as R)
    // Return the final result
    return resultVar.value as R
  }

  companion object {
    fun <R> reset(ctx: CoroutineContext, f: suspend DelimitedScope<R>.() -> R): R = DelimContScope(ctx, f).invoke()

    @Suppress("ClassName")
    private object EMPTY_VALUE
  }
}
