package arrow.fx.coroutines.stream

import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.Promise
import arrow.fx.coroutines.StreamSpec
import arrow.fx.coroutines.assertCancellable
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int

class CancellationTest : StreamSpec(iterations = 20_000, depth = 0..200, spec = {

  "constant" {
    checkAll(Arb.int()) { i ->
      Stream.constant(i).assertCancellable()
    }
  }

  "bracketed stream" {
    var count = 0
    var cancelCalled = 0
    checkAll(Arb.int()) { i ->
      val exitCase = Promise<ExitCase>()

      assertCancellable { latch ->
        Stream.bracketCase(
          {
//            println("Acquired")
            latch.complete(Unit)
          },
          { _, ex ->
//            println("Completed($ex)")
            cancelCalled++
            exitCase.complete(ex)
          }
        ).flatMap { Stream.constant(i) }
//          .interruptWhen { never() }
      }

      count++
//      println("-\n")
      exitCase.get() shouldBe ExitCase.Cancelled
    }

    println("Ran $count times")
    println("Cancelled got called: $cancelCalled")
  }

//  "bracketed stream" {
//    var count = 0
//    checkAll(Arb.int()) { i ->
//      val exitCase = Promise<ExitCase>()
//      Stream.bracketCase(
//        { println("Acquired") },
//        { _, ex -> exitCase.complete(ex).also { println("Completed($ex)") } }
//      ).flatMap { Stream.constant(i) }
//        .assertCancellable()
//
//      println("count: ${count++}")
//      exitCase.get() shouldBe ExitCase.Cancelled
//    }
//  }
})
