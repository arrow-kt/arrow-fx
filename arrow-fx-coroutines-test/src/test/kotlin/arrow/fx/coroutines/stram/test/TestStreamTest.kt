package arrow.fx.coroutines.stram.test

import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.concurrent.Queue
import arrow.fx.coroutines.stream.test.testStream
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe

class TestStreamTest : FreeSpec({

  "builtin assertions" - {

    "expects nothing when stream is empty" {
      testStream {
        Stream.empty<String>().capture()
        expectNothingMore()
      }
    }

    "expects single item" {
      testStream {
        Stream.just("Hello").capture()
        expect("Hello")
      }
    }

    "expects multiple items" {
      testStream {
        Stream("Hello", "World").capture()

        expectAll("Hello", "World")
      }
    }

    "expects items in order" {
      testStream {
        val queue = Queue.unbounded<String>()
        queue.dequeue().capture()

        queue.enqueue1("Hello")
        expect("Hello")
        queue.enqueue1("World")
        expect("World")
      }
    }

    "expects exception" {
      testStream {
        val expectedException = RuntimeException()

        Stream.raiseError<RuntimeException>(expectedException).capture()

        expectException(expectedException)
      }
    }

    "handle multiple streams" {
      testStream {
        Stream("World").capture()
        Stream("Hello").capture()

        expectAll("Hello", "World")
      }
    }

    "expects with predicate" {
      testStream {
        Stream("input").capture()

        expectThat<String> { it.startsWith("in") }
      }
    }

    "capture in order" {
      testStream {
        captureInOrder(Stream("Hello"), Stream("World"))

        expectInOrder("Hello", "World")
      }
    }
  }

  "custom assertions" - {

    "expects nothing when stream is empty" {
      testStream {
        Stream.empty<String>().capture()
        nextOrNull() shouldBe null
      }
    }

    "expects single item" {
      testStream {
        Stream.just("Hello").capture()
        next() shouldBe "Hello"
      }
    }

    "expects multiple items" {
      testStream {
        Stream("Hello", "World").capture()

        next(2).shouldContainExactly("Hello", "World")
      }
    }

    "expects items in order" {
      testStream {
        val queue = Queue.unbounded<String>()
        queue.dequeue().capture()

        queue.enqueue1("Hello")
        next() shouldBe "Hello"
        queue.enqueue1("World")
        next() shouldBe "World"
      }
    }

    "expects exception" {
      testStream {
        val expectedException = RuntimeException()

        Stream.raiseError<RuntimeException>(expectedException).capture()

        nextException() shouldBe expectedException
      }
    }

    "handle multiple streams" {
      testStream {
        Stream("World").capture()
        Stream("Hello").capture()

        next(2) shouldContainAll listOf("Hello", "World")
      }
    }

    "capture in order" {
      testStream {
        captureInOrder(Stream("Hello"), Stream("World"))

        next(2) shouldBe arrayOf("Hello", "World")
      }
    }
  }

})
