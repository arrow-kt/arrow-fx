package arrow.fx.coroutines

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.longs.shouldBeGreaterThanOrEqual
import io.kotest.matchers.longs.shouldBeLessThan
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.positiveInts
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.reduce

class FlowTest : ArrowFxSpec(spec = {

  "Retry - flow fails" {
    checkAll(Arb.int(), Arb.positiveInts(10)) { a, n ->
      var counter = 0
      val e = shouldThrow<RuntimeException> {
        flow {
          emit(a)
          if (++counter <= 11) throw RuntimeException("Bang!")
        }.retry(Schedule.recurs(n))
          .collect()
      }
      e.message shouldBe "Bang!"
    }
  }

  "Retry - flow succeeds" {
    checkAll(Arb.int(), Arb.int(5, 10)) { a, n ->
      var counter = 0
      val sum = flow {
        emit(a)
        if (++counter <= 5) throw RuntimeException("Bang!")
      }.retry(Schedule.recurs(n))
        .reduce { acc, int -> acc + int }

      sum shouldBe a * 6
    }
  }

  // Note: this takes 50-60 sec to run
  "Retry - schedule with delay" {
    checkAll(Arb.int(), Arb.int(50, 100)) { a, delayMs ->
      val timestamps = mutableListOf<Long>()
      shouldThrow<RuntimeException> {
        flow {
          emit(a)
          timestamps.add(System.currentTimeMillis())
          throw RuntimeException("Bang!")
        }
          .retry(Schedule.recurs<Throwable>(2) and Schedule.spaced(delayMs.milliseconds))
          .collect()
      }
      timestamps.size shouldBe 3

      // total run should be between start time + delay * 2 AND start + tolerance %
      val min = timestamps.first() + (delayMs.milliseconds * 2).millis
      val max = min + delayMs / 6

      timestamps.last() shouldBeGreaterThanOrEqual min
      timestamps.last() shouldBeLessThan max
    }
  }
})
