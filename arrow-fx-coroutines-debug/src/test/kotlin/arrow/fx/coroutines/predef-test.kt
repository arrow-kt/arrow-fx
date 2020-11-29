@file:Suppress("UNUSED", "INVISIBLE_REFERENCE", "INVISIBLE_MEMBER")
package arrow.fx.coroutines

import arrow.fx.coroutines.debug.DebugProbes
import arrow.fx.coroutines.debug.resetCoroutineId
import java.io.ByteArrayOutputStream
import java.io.PrintStream

fun withDebugProbe(
  sanitizeStackTraces: Boolean = true,
  enableCreationStackTraces: Boolean = true,
  f: suspend () -> Unit
): Unit = Environment().unsafeRunSync {
  DebugProbes.sanitizeStackTraces = sanitizeStackTraces
  DebugProbes.enableCreationStackTraces = enableCreationStackTraces
  resetCoroutineId()
  DebugProbes.install()
  f.invoke()
  DebugProbes.uninstall()
}

fun String.applyBackspace(): String {
  val array = toCharArray()
  val stack = CharArray(array.size)
  var stackSize = -1
  for (c in array) {
    if (c != '\b') {
      stack[++stackSize] = c
    } else {
      --stackSize
    }
  }

  return String(stack, 0, stackSize + 1)
}

fun String.trimStackTrace(): String =
  trimIndent()
    .replace(Regex(":[0-9]+"), "")
    .replace(Regex("#[0-9]+"), "")
    .replace(Regex("(?<=\tat )[^\n]*/"), "")
    .replace(Regex("\t"), "")
    .replace("sun.misc.Unsafe.", "jdk.internal.misc.Unsafe.") // JDK8->JDK11
    .applyBackspace()

val regexHashCode = Regex("@[0-9a-fA-F]+")

private fun String.sanitizeAddresses(): String =
  replace(regexHashCode, "")

inline fun verifyPartialDump(createdCoroutinesCount: Int, vararg frames: String, finally: () -> Unit) {
  try {
    verifyPartialDump(createdCoroutinesCount, *frames)
  } finally {
    finally()
  }
}

fun verifyPartialDump(createdCoroutinesCount: Int, vararg frames: String) {
  val baos = ByteArrayOutputStream()
  DebugProbes.dumpCoroutines(PrintStream(baos))
  val dump = baos.toString()
  val trace = dump.split("\n\n")
  val matches = frames.all { frame ->
    trace.any { tr -> tr.contains(frame) }
  }

  val debuggerInfo = DebugProbes.dumpDebuggerInfo()
  assert(createdCoroutinesCount == debuggerInfo.size) {
    "Expected $createdCoroutinesCount but found ${debuggerInfo.size}"
  }
  assert(matches) {
    "Expected to find ${frames.joinToString()} in the trace, but didn't match.\n\n$trace"
  }
}

inline fun verifyDump(vararg traces: String, ignoredCoroutine: String? = null, finally: () -> Unit) {
  try {
    verifyDump(*traces, ignoredCoroutine = ignoredCoroutine)
  } finally {
    finally()
  }
}

fun verifyDump(vararg traces: String, ignoredCoroutine: String? = null) {
  val baos = ByteArrayOutputStream()
  DebugProbes.dumpCoroutines(PrintStream(baos))
  val trace = baos.toString().split("\n\n")
  if (traces.isEmpty()) {
    val filtered = trace.filter { ignoredCoroutine == null || !it.contains(ignoredCoroutine) }
    assert(1 == filtered.count()) {
      "Expected lines in dump, but found none"
    }
    assert(filtered[0].startsWith("Coroutines dump")) {
      "Expected dump to start with \"Coroutines dump\", but found ${filtered[0]}"
    }
    return
  }

  trace
    .drop(1) // Drop "Coroutines dump 2020/11/28 20:16:31" line
    .filter { ignoredCoroutine != null && !it.contains(ignoredCoroutine) }
    .withIndex().forEach { (index, value) ->

      val expected = traces[index].applyBackspace().split("\n\t(Coroutine creation stacktrace)\n", limit = 2)
      val actual = value.applyBackspace().split("\n\t(Coroutine creation stacktrace)\n", limit = 2)
      assert(expected.size == actual.size) { "Creation stacktrace should be part of the expected input" }

      expected.withIndex().forEach { (index, trace) ->
        val actualTrace = actual[index].trimStackTrace().sanitizeAddresses()
        val expectedTrace = trace.trimStackTrace().sanitizeAddresses()

        assert(actualTrace == expectedTrace) {
          """
          |In part $index of ${expected.size - 1} expected: $expectedTrace
          |but found: $actualTrace
        """.trimMargin()
        }
      }
    }
}
