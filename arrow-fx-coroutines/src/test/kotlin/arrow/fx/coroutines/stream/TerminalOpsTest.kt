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
      stream.expect(toEmit = 0).drain()
    }
    "first" {
      stream.firstOrNull() shouldBe null
    }
    "first or else" {
      shouldThrow<IllegalStateException> {
        stream.firstOrElse { error("Oops!") }
      }.shouldHaveMessage("Oops!")
    }
    "first or alternative" {
      stream.firstOrElse { 42 } shouldBe 42
    }
    "last or null" {
      stream.lastOrNull() shouldBe null
    }
    "last or alternative" {
        stream.lastOrElse { 42 } shouldBe 42
    }
    "last or error" {
      shouldThrow<IllegalStateException> {
        stream.lastOrElse { error("Oops!") }
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
      stream.expect(toEmit = 1).drain()
    }
    "first" {
      stream.firstOrNull() shouldBe 42
    }
    "first or alternative" {
      stream.firstOrElse { 43 } shouldBe 42
    }
    "first or error" {
      stream.firstOrElse { error("Oops!") } shouldBe 42
    }
    "last or null" {
      stream.lastOrNull() shouldBe 42
    }
    "last or alternative" {
      stream.lastOrElse { 43 } shouldBe 42
    }
    "last or error" {
      stream.lastOrElse { error("Oops!") } shouldBe 42
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
      stream.expect(toEmit = 3).drain()
    }
    "first" {
      stream.firstOrNull() shouldBe 40
    }
    "first or alternative" {
      stream.firstOrElse { 43 } shouldBe 40
    }
    "first or error" {
      stream.firstOrElse { error("Oops!") } shouldBe 40
    }
    "last or null" {
      stream.lastOrNull() shouldBe 42
    }
    "last or alternative" {
      stream.lastOrElse { 43 } shouldBe 42
    }
    "last or error" {
      stream.lastOrElse { error("Oops!") } shouldBe 42
    }
  }

  "infinite stream" - {
    val stream = Stream.iterateEffect(0) { it + 1 }
      .timeout(Duration(1000, TimeUnit.SECONDS))
    "to list" {
      shouldThrow<TimeoutException> { stream.toList() }
    }
    "to set" {
      shouldThrow<TimeoutException> { stream.toSet() }
    }
    "drain" {
      shouldThrow<TimeoutException> { stream.drain() }
    }
    "first" {
      stream.firstOrNull() shouldBe 0
    }
    "first or alternative" {
      stream.firstOrElse { 42 } shouldBe 0
    }
    "first or error" {
      stream.firstOrElse { error("Oops!") } shouldBe 0
    }
    "last or null" {
      shouldThrow<TimeoutException> { stream.lastOrNull() }
    }
    "last or alternative" {
      shouldThrow<TimeoutException> { stream.lastOrElse { 42 } }
    }
    "last or error" {
      shouldThrow<TimeoutException> { stream.lastOrElse { error("Oops!") } }
    }
  }
})

private fun <O> Stream<O>.expect(toEmit: Int) =
  fold(0) { counter, _ -> counter + 1 }
    .effectTap { counter -> counter shouldBe toEmit }
