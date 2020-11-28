package arrow.fx.coroutines

import arrow.core.Either
import arrow.core.identity
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.drain
import arrow.fx.coroutines.stream.parJoinUnbounded
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.bind
import io.kotest.property.arbitrary.choose
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.map
import io.kotest.property.forAll
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.startCoroutineUninterceptedOrReturn

/**
 * Computation pool implicit usages:
 * Stream.Companion.effect (through raceN)
 * Stream.Companion.cancellable (through ForkAndForget)
 * Stream.interruptWhen (through ForkAndForget)
 * ForkScoped (through ForkConnected)
 * Scope.InterruptContext.childContext (through ForkConnected twice)
 * SignallingAtomic (almost every method through ForkConnected)
 */

fun <O> Arb.Companion.stream(
  arb: Arb<O>,
  range: IntRange
): Arb<Stream<O>> =
  Arb.choose(
    10 to Arb.list(arb, range).map { os ->
      Stream.iterable(os)
    },
    10 to Arb.list(arb, range).map { os ->
      Stream.iterable(os).unchunk()
    },
    5 to arb.map { fo -> Stream.effect { fo } },
    1 to Arb.bind(Arb.suspended(arb), Arb.list(arb, range), Arb.suspended(Arb.unit())) { acquire, use, release ->
      Stream.bracketCase(acquire, { _, _ -> release.invoke() }).flatMap { Stream.iterable(use) }
    }
  )

class MyTest : ConcurrencyTestSpec({
  "!ForkConnected doesn't cancel its parent" {
    nonFlaky {
      val cancelled = Promise<Unit>()

      val parent = ForkAndForget() {
        val f = ForkConnected() {
          never<Unit>()
        }
        val res = parTupledN({
          cancelled.get()
        }, {
          f.cancel(); cancelled.complete(Unit)
        })
        cancelBoundary()
        10
      }
      parent.join() shouldBe 10
    }
  }
  "!test" {
    nonFlaky {
      var res = 0
      raceN({
        res = 1
      }, {
        res = 2
      })
      res
    }
  }
  "outer failed" {
    forAll(Arb.throwable(), Arb.stream(Arb.int(), 0..100)) { e, s ->
      nonFlaky {
        assertThrowable {
          Stream(s, Stream.raiseError(e))
            .parJoinUnbounded()
            .drain()
        } shouldBe e
      }
      true
    }
  }
})

sealed class ConcurrencyTestResult {
  data class Success(
    val runs: Int,
    val results: Map<DecisionChain, Any?>
  ) : ConcurrencyTestResult()

  data class Failure(
    val runs: Int,
    val results: Map<DecisionChain, Any?>,
    val lastPath: DecisionChain? = null,
    val failure: Throwable
  ) : ConcurrencyTestResult()

  fun ensureDeterministic(): ConcurrencyTestResult = when (this) {
    is Failure -> this
    is Success ->
      if (results.values.toSet().size == 1) this
      else Failure(
        runs, results, failure = NonDeterminism(results)
      )
  }

  fun render(): Unit = when (this) {
    is Failure -> {
      println("Failed after $runs")
      println("Unique paths ${results.map { (c, _) -> c.toString() }.toSet().size}")
      println("Unique results ${results.map { (_, r) -> r }.toSet().size}")
      println("Final Path: $lastPath")
    }
    is Success -> {
      println("Runs: $runs")
      results.forEach { (chain, res) ->
        println("$chain: $res")
      }
      println("Unique paths ${results.map { (c, _) -> c.toString() }.toSet().size}")
      println("Unique results ${results.map { (_, r) -> r }.toSet().size}")
    }
  }

  fun throwFailure(): Unit = when (this) {
    is Failure -> throw failure
    is Success -> Unit
  }
}

class NonDeterminism(results: Map<DecisionChain, Any?>) : Throwable()

open class ConcurrencyTestSpec(spec: ConcurrencyTestSpec.() -> Unit = {}) : FreeSpec() {
  init {
    spec()
  }

  fun nonFlaky(f: suspend () -> Any?): Unit =
    executeTest(f).let {
      it.render()
      it.throwFailure()
    }

  fun deterministic(f: suspend () -> Any?): Unit =
    executeTest(f).ensureDeterministic().let {
      it.render()
      it.throwFailure()
    }

  object EMPTY

  fun executeTest(f: suspend () -> Any?): ConcurrencyTestResult {
    val results = mutableListOf<Pair<DecisionChain, Any?>>()
    var chain: DecisionChain? = null
    var runs = 0
    do {
      runs++
      val service = TestExecutorService(chain)
      val serviceCtx = TestExecutorServiceCtx(service)

      var result: Either<Throwable, Any?> = Either.right(EMPTY)
      service.execute {
        try {
          f.startCoroutineUnintercepted(Continuation(serviceCtx + DefaultContext(serviceCtx)) {
            result = it.fold({ Either.right(it) }, { Either.left(it) })
          })
        } catch (t: Throwable) {
          result = Either.left(t)
        }
      }

      /*
      Why not yield and wait for a result?
        Well we may have scheduled tasks left and we don't really know what to do with them:
         a) run them, this takes away scheduler variety
         b) wait first, we don't know if we wait for a scheduled task!
         c) run, save blocking as reason tho and wait later, but that again runs into b) as well
          also we never really know how long we should wait
       */
      result.fold({}, { if (it === EMPTY) throw IllegalStateException("No result produced!") })

      // append new decision chain to old one
      if (service.newDecisionChain !== null)
        chain = (chain + service.newDecisionChain!!)

      val noErr = result.fold({
        return@executeTest ConcurrencyTestResult.Failure(runs, results.toMap(), chain, it)
      }, ::identity)

      if (chain === null) return@executeTest ConcurrencyTestResult.Success(runs, results.toMap())
      // save together with result
      results.add(chain to noErr)
      // advance chain
      chain = chain.advance()
    } while (chain != null)

    return ConcurrencyTestResult.Success(runs, results.toMap())
  }
}

class DecisionChain(var decision: Decision, var prev: DecisionChain? = null, var next: DecisionChain? = null) {
  fun advance(): DecisionChain? {
    if (next === null) {
      return if (
        decision is Decision.SWITCH
        && ((decision as Decision.SWITCH).max == (decision as Decision.SWITCH).to)
      ) null
      else DecisionChain(decision.advance())
    }

    val root = DecisionChain(decision.copy())
    var currCopy = root
    var curr: DecisionChain? = this.next
    while (curr !== null) {
      val dec = curr.decision.copy()
      currCopy.next = DecisionChain(dec, currCopy)
      currCopy = currCopy.next!!
      curr = curr.next
    }

    // there check the last one and see how we can advance
    while (
      currCopy.decision is Decision.SWITCH
      && (currCopy.decision as Decision.SWITCH).max == (currCopy.decision as Decision.SWITCH).to
    ) {
      if (currCopy.prev == null) return null
      currCopy.prev!!.next = null
      currCopy = currCopy.prev!!
    }

    currCopy.decision = currCopy.decision.advance()
    return root
  }

  override fun toString(): String {
    val list = mutableListOf<Decision>()
    var curr: DecisionChain? = this
    while (curr !== null) {
      list.add(curr.decision)
      curr = curr.next
    }
    return list.joinToString(", ")
  }
}

operator fun DecisionChain?.plus(other: DecisionChain): DecisionChain {
  if (this === null) return other
  var curr = this!!
  while (curr.next !== null) curr = curr.next!!
  curr.next = other
  other.prev = curr
  return this
}

sealed class Decision {
  fun copy(): Decision = when (this) {
    is NO_SWITCH -> NO_SWITCH(max)
    is SWITCH -> SWITCH(to, max)
  }

  fun advance(): Decision = when (this) {
    is NO_SWITCH -> SWITCH(0, max)
    is SWITCH -> SWITCH(to + 1, max)
  }

  data class NO_SWITCH(val max: Int) : Decision()
  data class SWITCH(val to: Int, val max: Int) : Decision()
}

class TestExecutorService(
  var decisionChain: DecisionChain? = null
) : ExecutorService {

  private var active = false

  var newDecisionChain: DecisionChain? = null
  val queue = mutableListOf<Runnable>()

  override fun execute(p0: Runnable) {
    if (active) {
      scheduleOrRun(p0)
    } else {
      active = true
      p0.run()

      // This is problematic if we are currently waiting for a different thread. Can we deal with this?
      while (queue.isNotEmpty())
        drainQueue()
      active = false
    }
  }

  fun drainQueue(): Unit {
    if (decisionChain == null) {
      if (newDecisionChain == null) {
        newDecisionChain = DecisionChain(Decision.SWITCH(0, queue.size - 1))
      } else {
        var last = newDecisionChain!!
        while (last.next != null) last = last.next!!
        last.next = DecisionChain(Decision.SWITCH(0, queue.size - 1), last)
      }
      queue.removeAt(0).run()
      return
    } else {
      val dec = decisionChain!!.decision
      decisionChain = decisionChain!!.next
      when (dec) {
        is Decision.NO_SWITCH -> {
          throw IllegalStateException("Unexpected...")
        }
        is Decision.SWITCH -> {
          queue.removeAt(dec.to).run()
          return
        }
      }
    }
  }

  fun scheduleOrRun(r: Runnable): Unit {
    if (decisionChain == null) {
      if (newDecisionChain == null) {
        newDecisionChain = DecisionChain(Decision.NO_SWITCH(queue.size), null)
      } else {
        var last = newDecisionChain!!
        while (last.next != null) last = last.next!!
        last.next = DecisionChain(Decision.NO_SWITCH(queue.size), last)
      }
      queue.add(0, r)
      return
    } else {
      val dec = decisionChain!!.decision
      decisionChain = decisionChain!!.next
      when (dec) {
        is Decision.NO_SWITCH -> {
          queue.add(0, r)
          return
        }
        is Decision.SWITCH -> {
          if (dec.to == 0) {
            r.run()
            return
          } else {
            queue.add(0, r)
            queue.removeAt(dec.to).run()
            return
          }
        }
      }
    }
  }

  override fun shutdown() {
    TODO("Not yet implemented")
  }

  override fun shutdownNow(): MutableList<Runnable> {
    TODO("Not yet implemented")
  }

  override fun isShutdown(): Boolean {
    TODO("Not yet implemented")
  }

  override fun isTerminated(): Boolean {
    TODO("Not yet implemented")
  }

  override fun awaitTermination(p0: Long, p1: TimeUnit): Boolean {
    TODO("Not yet implemented")
  }

  override fun <T : Any?> submit(p0: Callable<T>): Future<T> {
    TODO("Not yet implemented")
  }

  override fun <T : Any?> submit(p0: Runnable, p1: T): Future<T> {
    TODO("Not yet implemented")
  }

  override fun submit(p0: Runnable): Future<*> {
    TODO("Not yet implemented")
  }

  override fun <T : Any?> invokeAll(p0: MutableCollection<out Callable<T>>): MutableList<Future<T>> {
    TODO("Not yet implemented")
  }

  override fun <T : Any?> invokeAll(p0: MutableCollection<out Callable<T>>, p1: Long, p2: TimeUnit): MutableList<Future<T>> {
    TODO("Not yet implemented")
  }

  override fun <T : Any?> invokeAny(p0: MutableCollection<out Callable<T>>): T {
    TODO("Not yet implemented")
  }

  override fun <T : Any?> invokeAny(p0: MutableCollection<out Callable<T>>, p1: Long, p2: TimeUnit): T {
    TODO("Not yet implemented")
  }

}

private class TestExecutorServiceCtx(val service: ExecutorService) :
  AbstractCoroutineContextElement(ContinuationInterceptor),
  ContinuationInterceptor {

  override fun <T> interceptContinuation(continuation: Continuation<T>): Continuation<T> =
    TestExecutorContinuation(service, continuation.context.fold(continuation) { cont, element ->
      if (element != this@TestExecutorServiceCtx && element is ContinuationInterceptor)
        element.interceptContinuation(cont) else cont
    })

}

private class TestExecutorContinuation<T>(
  val pool: ExecutorService,
  val cont: Continuation<T>
) : Continuation<T> {
  override val context: CoroutineContext = cont.context

  override fun resumeWith(result: Result<T>) =
    pool.execute { cont.resumeWith(result) }
}
