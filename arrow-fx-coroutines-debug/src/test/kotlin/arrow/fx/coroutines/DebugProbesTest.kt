package arrow.fx.coroutines

import arrow.fx.coroutines.debug.DebugProbes
import org.junit.Test
import kotlin.coroutines.Continuation
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.intrinsics.createCoroutineUnintercepted

class DebugProbesTest {

  @Test
  fun suspendedCoroutine(): Unit = withDebugProbe {
    val fiber = ForkConnected(EmptyCoroutineContext) { never<Unit>() }
    verifyPartialDump(1, "state: SUSPENDED") {
      fiber.cancel()
    }
  }

  @Test
  fun runningCoroutine(): Unit = withDebugProbe {
    val fiber = ForkConnected {
      while (true) {
        cancelBoundary()
      }
    }

    verifyPartialDump(1, "state: RUNNING") {
      fiber.cancel()
    }
  }

  @Test
  fun createdCoroutine(): Unit = withDebugProbe {
    suspend { 1 }
      .createCoroutineUnintercepted(Continuation(EmptyCoroutineContext) { })
    verifyPartialDump(1, "state: CREATED")
  }
}

fun withDebugProbe(
  sanitizeStackTraces: Boolean = true,
  enableCreationStackTraces: Boolean = true,
  f: suspend () -> Unit
): Unit = Environment().unsafeRunSync {
  DebugProbes.sanitizeStackTraces = sanitizeStackTraces
  DebugProbes.enableCreationStackTraces = enableCreationStackTraces
  DebugProbes.install()
  f.invoke()
  DebugProbes.uninstall()
}
