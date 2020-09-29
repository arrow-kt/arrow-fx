package arrow.fx.coroutines.stream

import arrow.fx.coroutines.Duration
import arrow.fx.coroutines.StreamSpec
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class TerminalOpsTest : StreamSpec(spec = {
  "empty stream" - {
    val stream = Stream.empty<Int>()
    "to list" {
      stream.toList() shouldBe emptyList()
    }
    "to set" {
      stream.toSet() shouldBe emptySet()
    }
    "drain" {
      stream.limit(callsTo = 0).drain()
    }
    "first or null" {
      stream.firstOrNull() shouldBe null
    }
    "first or error" {
      shouldThrow<NoSuchElementException> {
        stream.firstOrError()
      }
    }
    "first or custom error" {
      shouldThrow<IllegalStateException> {
        stream.firstOrError { error("Oops!") }
      }.shouldHaveMessage("Oops!")
    }
    "last or null" {
      stream.lastOrNull() shouldBe null
    }
    "last or error" {
      shouldThrow<NoSuchElementException> {
        stream.lastOrError()
      }
    }
    "last or custom error" {
      shouldThrow<IllegalStateException> {
        stream.lastOrError { error("Oops!") }
      }.shouldHaveMessage("Oops!")
    }
  }

  "single stream" - {
    val stream = Stream.just(42)
    "to list" {
      stream.toList() shouldBe listOf(42)
    }
    "to set" {
      stream.toSet() shouldBe setOf(42)
    }
    "drain" {
      stream.limit(callsTo = 1).drain()
    }
    "first or null" {
      stream.firstOrNull() shouldBe 42
    }
    "first or error" {
      stream.firstOrError() shouldBe 42
    }
    "first or custom error" {
      stream.firstOrError { error("Oops!") } shouldBe 42
    }
    "last or null" {
      stream.lastOrNull() shouldBe 42
    }
    "last or error" {
      stream.lastOrError() shouldBe 42
    }
    "last or custom error" {
      stream.lastOrError { error("Oops!") } shouldBe 42
    }
  }

  "multiple items stream" - {
    val stream = Stream.range(40..42)
    "to list" {
      stream.toList() shouldBe listOf(40, 41, 42)
    }
    "to set" {
      stream.toSet() shouldBe setOf(40, 41, 42)
    }
    "drain" {
      stream.limit(callsTo = 3).drain()
    }
    "first or null" {
      stream.firstOrNull() shouldBe 40
    }
    "first or error" {
      stream.firstOrError() shouldBe 40
    }
    "first or custom error" {
      stream.firstOrError { error("Oops!") } shouldBe 40
    }
    "last or null" {
      stream.lastOrNull() shouldBe 42
    }
    "last or error" {
      stream.lastOrError() shouldBe 42
    }
    "last or custom error" {
      stream.lastOrError { error("Oops!") } shouldBe 42
    }
  }

  "infinite stream" - {
    val stream = Stream.iterateEffect(0) { it + 1 }
      .timeout(Duration(10, TimeUnit.MILLISECONDS))
    "to list" {
      shouldThrow<TimeoutException> { stream.toList() }
    }
    "to set" {
      shouldThrow<TimeoutException> { stream.toSet() }
    }
    "drain" {
      shouldThrow<TimeoutException> { stream.drain() }
    }
    "first or null" {
      stream.firstOrNull() shouldBe 0
    }
    "first or error" {
      stream.firstOrError() shouldBe 0
    }
    "first or custom error" {
      stream.firstOrError { error("Oops!") } shouldBe 0
    }
    "last or null" {
      shouldThrow<TimeoutException> { stream.lastOrNull() }
    }
    "last or error" {
      shouldThrow<TimeoutException> { stream.lastOrError() }
    }
    "last or custom error" {
      shouldThrow<TimeoutException> { stream.lastOrError { error("Oops!") } }
    }
  }
})

private fun <O> Stream<O>.limit(callsTo: Int): Stream<Int> =
  fold(0) { counter, _ -> counter + 1 }
    .effectTap { counter -> counter shouldBe callsTo }
