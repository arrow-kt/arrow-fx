package arrow.fx.coroutines.stram.test

import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.test.testStreamCompat
import org.junit.jupiter.api.Test

class TestStreamTest {

  @Test
  fun `expects single item`(): Unit = testStreamCompat {
    Stream.just("Hello").capture()
    expect("Hello")
  }

}
