package arrow.fx.coroutines.stram.test

import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.concurrent.Queue
import arrow.fx.coroutines.stream.test.testStreamCompat
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class TestStreamTest : StringSpec({

  "expects nothing when stream is empty" {
    testStreamCompat {
      Stream.empty<String>().capture()
      expectNothingMore()
    }
  }

  "expects single item" {
    testStreamCompat {
      Stream.just("Hello").capture()
      expect("Hello")
    }
  }

  "expects multiple items" {
    testStreamCompat {
      Stream("Hello", "World").capture()

      expect("Hello", "World")
    }
  }

  "expects items in order" {
    testStreamCompat {
      val queue = Queue.unbounded<String>()
      queue.dequeue().capture()

      queue.enqueue1("Hello")
      expect("Hello")
      queue.enqueue1("World")
      expect("World")
    }
  }

  "expects exception" {
    testStreamCompat {
      val expectedException = RuntimeException()

      Stream.raiseError<RuntimeException>(expectedException).capture()

      expectException(expectedException)
    }
  }

  // TODO: Fix flakyness
  "handle multiple streams" {
    testStreamCompat {
      Stream("Hello").capture()
      Stream("World").capture()

      expect("Hello", "World")
    }
  }

})
