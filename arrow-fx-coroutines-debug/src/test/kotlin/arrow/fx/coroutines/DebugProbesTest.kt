@file:Suppress("UNUSED", "INVISIBLE_REFERENCE", "INVISIBLE_MEMBER")

package arrow.fx.coroutines

import arrow.fx.coroutines.debug.newCoroutineContext
import arrow.fx.coroutines.debug.toDebugString
import org.junit.Test
import kotlin.coroutines.Continuation
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.intrinsics.createCoroutineUnintercepted

class DebugProbesTest {

  @Test
  fun suspendedCoroutine(): Unit = withDebugProbe {
    val fiber = ForkConnected(EmptyCoroutineContext) { never<Unit>() }
    verifyPartialDump(1, "coroutine#1:FiberContinuation", "state: SUSPENDED") {
      fiber.cancel()
    }
  }

  @Test
  fun runningCoroutine(): Unit = withDebugProbe {
    val latch = Promise<Unit>()
    val fiber = ForkConnected {
      while (true) {
        latch.complete(Unit) // Coroutine is running
        cancelBoundary()
      }
    }

    latch.get()

    verifyPartialDump(1, "coroutine#1:FiberContinuation", "state: RUNNING") {
      fiber.cancel()
    }
  }

  @Test
  fun createdCoroutine(): Unit = withDebugProbe {
    suspend { 1 }
      .createCoroutineUnintercepted(object : Continuation<Int> {
        override val context: CoroutineContext = newCoroutineContext(EmptyCoroutineContext)
        override fun resumeWith(result: Result<Int>) = Unit
        override fun toString(): String =
          toDebugString("Continuation")
      })

    verifyPartialDump(1, "coroutine#1:Continuation, state: CREATED")
  }

  @Test
  fun evalOnDispatch(): Unit = withDebugProbe {
    evalOn(IOPool) {
      // A single coroutine is created since we
      verifyPartialDump(1, "coroutine#1:DispatchableCoroutine", "state: RUNNING")
    }
  }

  @Test
  fun evalOnNoDispatch(): Unit = withDebugProbe {
    evalOn(ComputationPool) {
      // When dispatching on the same ctx/ContinuationInterceptor then no coroutine is created.
      // It's considered to run on the same coroutine
      verifyPartialDump(0)
    }
  }

  @Test
  fun parMap2(): Unit = withDebugProbe {
    val fiber = ForkConnected(EmptyCoroutineContext) {
      parMapN(EmptyCoroutineContext, { never<Unit>() }, { never<Unit>() }, ::Pair)
    }

    // 3 Coroutines are created, the fiber and 2 inside parMapN
    verifyPartialDump(3,
      "coroutine#1:FiberContinuation",
      "coroutine#2:FiberContinuation",
      "coroutine#3:FiberContinuation"
    ) {
      fiber.cancel()
    }
  }

  @Test
  fun parMap3(): Unit = withDebugProbe {
    val fiber = ForkConnected(EmptyCoroutineContext) {
      parMapN(EmptyCoroutineContext, { never<Unit>() }, { never<Unit>() }, { never<Unit>() }, ::Triple)
    }

    // 4 Coroutines are created, the fiber and 3 inside parMapN
    verifyPartialDump(4,
      "coroutine#1:FiberContinuation",
      "coroutine#2:FiberContinuation",
      "coroutine#3:FiberContinuation",
      "coroutine#4:FiberContinuation"
    ) {
      fiber.cancel()
    }
  }

  @Test
  fun race2(): Unit = withDebugProbe {
    val fiber = ForkConnected(EmptyCoroutineContext) {
      raceN(EmptyCoroutineContext, { never<Unit>() }, { never<Unit>() })
    }

    // 4 Coroutines are created, the fiber and 2 inside raceN
    verifyPartialDump(3,
      "coroutine#1:FiberContinuation",
      "coroutine#2:FiberContinuation",
      "coroutine#3:FiberContinuation"
    ) {
      fiber.cancel()
    }
  }

  @Test
  fun race3(): Unit = withDebugProbe {
    val fiber = ForkConnected(EmptyCoroutineContext) {
      raceN(EmptyCoroutineContext, { never<Unit>() }, { never<Unit>() }, { never<Unit>() })
    }

    // 4 Coroutines are created, the fiber and 3 inside raceN
    verifyPartialDump(4,
      "coroutine#1:FiberContinuation",
      "coroutine#2:FiberContinuation",
      "coroutine#3:FiberContinuation",
      "coroutine#4:FiberContinuation"
    ) {
      fiber.cancel()
    }
  }

  @Test
  fun racePair(): Unit = withDebugProbe {
    val fiber = ForkConnected(EmptyCoroutineContext) {
      racePair(EmptyCoroutineContext, { never<Unit>() }, { never<Unit>() })
    }

    // 3 Coroutines are created, the fiber and 2 inside racePair
    verifyPartialDump(3,
      "coroutine#1:FiberContinuation",
      "coroutine#2:FiberContinuation",
      "coroutine#3:FiberContinuation"
    ) {
      fiber.cancel()
    }
  }

  @Test
  fun raceTriple(): Unit = withDebugProbe {
    val fiber = ForkConnected(EmptyCoroutineContext) {
      raceTriple(EmptyCoroutineContext, { never<Unit>() }, { never<Unit>() }, { never<Unit>() })
    }

    // 4 Coroutines are created, the fiber and 3 inside raceTriple
    verifyPartialDump(4,
      "coroutine#1:FiberContinuation",
      "coroutine#2:FiberContinuation",
      "coroutine#3:FiberContinuation",
      "coroutine#4:FiberContinuation"
    ) {
      fiber.cancel()
    }
  }
}
