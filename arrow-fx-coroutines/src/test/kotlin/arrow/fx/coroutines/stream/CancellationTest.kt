package arrow.fx.coroutines.stream

import arrow.fx.coroutines.ArrowFxSpec
import arrow.fx.coroutines.Atomic
import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.Promise
import arrow.fx.coroutines.assertCancellable
import arrow.fx.coroutines.rethrow
import io.kotest.assertions.failure
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int

class CancellationTest : ArrowFxSpec(spec = {

  "constant" {
    checkAll(Arb.int()) { i ->
      Stream.constant(i).assertCancellable()
    }
  }

  "bracketed stream" {
    checkAll(Arb.int()) { i ->
      val exitCase = Promise<ExitCase>()

      assertCancellable { latch ->
        Stream.bracketCase(
          { latch.complete(Unit) },
          { _, ex ->
            exitCase.complete(ex)
              .mapLeft { failure("Bracket finalizer may only be called once") }
              .rethrow()
          }
        ).flatMap { Stream.constant(i) }
      }

      exitCase.get() shouldBe ExitCase.Cancelled
    }
  }

  "bracketed stream calls finalizer once" {
    checkAll(Arb.int()) { i ->
      val count = Atomic(0)

      assertCancellable { latch ->
        Stream.bracketCase(
          { latch.complete(Unit) },
          { _, ex -> count.update(Int::inc) }
        ).flatMap { Stream.constant(i) }
      }

      count.get() shouldBe 0
    }
  }
})
