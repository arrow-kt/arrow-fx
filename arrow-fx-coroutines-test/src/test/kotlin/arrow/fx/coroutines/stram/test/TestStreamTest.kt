package arrow.fx.coroutines.stram.test

import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.test.testStreamCompat
import io.kotest.core.spec.style.StringSpec

class TestStreamTest : StringSpec({

  "expects single item" {
    testStreamCompat {
      Stream.just("Hello").capture()
      expect("Hello")
    }
  }

//  @Test
//  fun `expects single item`(): Unit = testStreamCompat {
//    Stream.just("Hello").capture()
//    expect("Hello")
//  }

})
