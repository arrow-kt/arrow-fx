@file:Suppress("UNUSED", "INVISIBLE_REFERENCE", "INVISIBLE_MEMBER")
package arrow.fx.coroutines.debug

import java.io.PrintStream
import kotlin.coroutines.Continuation

// Stubs which are injected as coroutine probes. Require direct match of signatures
internal fun probeCoroutineResumed(frame: Continuation<*>) =
  DebugProbesImpl.probeCoroutineResumed(frame)

internal fun probeCoroutineSuspended(frame: Continuation<*>) =
  DebugProbesImpl.probeCoroutineSuspended(frame)

internal fun <T> probeCoroutineCreated(completion: Continuation<T>): Continuation<T> =
    DebugProbesImpl.probeCoroutineCreated(completion)

object DebugProbes {

  /**
   * Whether coroutine creation stack traces should be sanitized.
   * Sanitization removes all frames from `kotlinx.coroutines` package except
   * the first one and the last one to simplify diagnostic.
   */
  var sanitizeStackTraces: Boolean
    get() = DebugProbesImpl.sanitizeStackTraces
    set(value) {
      DebugProbesImpl.sanitizeStackTraces = value
    }

  /**
   * Whether coroutine creation stack traces should be captured.
   * When enabled, for each created coroutine a stack trace of the current
   * thread is captured and attached to the coroutine.
   * This option can be useful during local debug sessions, but is recommended
   * to be disabled in production environments to avoid stack trace dumping overhead.
   */
  var enableCreationStackTraces: Boolean
    get() = DebugProbesImpl.enableCreationStackTraces
    set(value) {
      DebugProbesImpl.enableCreationStackTraces = value
    }

  /**
   * Installs a [DebugProbes] instead of no-op stdlib probes by redefining
   * debug probes class using the same class loader as one loaded [DebugProbes] class.
   */
  fun install() {
    DebugProbesImpl.install()
  }

  fun uninstall() {
    DebugProbesImpl.uninstall()
  }

  fun dumpDebuggerInfo(): List<CoroutineInfo> =
    DebugProbesImpl.dumpDebuggerInfo()
      .map(::CoroutineInfo)

  /**
   * Dumps all active coroutines into the given output stream, providing a consistent snapshot of all existing coroutines at the moment of invocation.
   * The output of this method is similar to `jstack` or a full thread dump. It can be used as the replacement to
   * "Dump threads" action.
   *
   * Example of the output:
   * ```
   * Coroutines dump 2018/11/12 19:45:14
   *
   * Coroutine "coroutine#42":StandaloneCoroutine{Active}@58fdd99, state: SUSPENDED
   *     at MyClass$awaitData.invokeSuspend(MyClass.kt:37)
   * (Coroutine creation stacktrace)
   *     at MyClass.createIoRequest(MyClass.kt:142)
   *     at MyClass.fetchData(MyClass.kt:154)
   *     at MyClass.showData(MyClass.kt:31)
   * ...
   * ```
   */
  fun dumpCoroutines(out: PrintStream = System.out): Unit =
    DebugProbesImpl.dumpCoroutines(out)

}
