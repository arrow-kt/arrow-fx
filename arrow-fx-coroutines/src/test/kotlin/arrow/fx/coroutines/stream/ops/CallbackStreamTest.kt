package arrow.fx.coroutines.stream.ops

import arrow.fx.coroutines.CancelToken
import arrow.fx.coroutines.ForkAndForget
import arrow.fx.coroutines.Promise
import arrow.fx.coroutines.Schedule
import arrow.fx.coroutines.StreamSpec
import arrow.fx.coroutines.assertThrowable
import arrow.fx.coroutines.milliseconds
import arrow.fx.coroutines.parTupledN
import arrow.fx.coroutines.sleep
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.callbackStream
import arrow.fx.coroutines.stream.cancellableCallbackStream
import arrow.fx.coroutines.stream.chunk
import arrow.fx.coroutines.stream.compile
import arrow.fx.coroutines.throwable
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.filter
import io.kotest.property.arbitrary.int
import kotlin.coroutines.Continuation
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.startCoroutine

class CallbackStreamTest : StreamSpec(iterations = 250, spec = {

  "callbackStream should be lazy" {
    var effect = 0
    Stream.callbackStream<Int> {
      effect += 1
    }

    effect shouldBe 0
  }

  "emits values" {
    Stream.callbackStream {
      emit(1)
      emit(2)
      emit(3)
    }
      .take(3)
      .compile()
      .toList() shouldBe listOf(1, 2, 3)
  }

  "emits varargs" {
    Stream.callbackStream {
      emit(1, 2, 3)
    }
      .take(3)
      .compile()
      .toList() shouldBe listOf(1, 2, 3)
  }

  "emits iterable" {
    Stream.callbackStream<Int> {
      emit(listOf(1, 2, 3))
    }
      .take(3)
      .compile()
      .toList() shouldBe listOf(1, 2, 3)
  }

  "emits chunks" {
    checkAll(Arb.chunk(Arb.int()).filter { it.isNotEmpty() }, Arb.chunk(Arb.int()).filter { it.isNotEmpty() }) { ch, ch2 ->
      Stream.callbackStream<Int> {
        emit(ch)
        emit(ch2)
        end()
      }
        .chunks().compile()
        .toList() shouldBe listOf(ch, ch2)
    }
  }

  "long running emission" {
    Stream.callbackStream {
      ForkAndForget {
        countToCallback(5, { it }) { emit(it) }
      }
    }
      .take(5)
      .compile()
      .toList() shouldBe listOf(1, 2, 3, 4, 5)
  }

  "emits and completes" {
    Stream.callbackStream {
      emit(1)
      emit(2)
      emit(3)
      end()
    }
      .compile()
      .toList() shouldBe listOf(1, 2, 3)
  }

  "forwards errors" {
    checkAll(Arb.throwable()) { e ->
      val s = Stream.callbackStream<Int> {
        throw e
      }
        .compile()

      assertThrowable {
        s.toList()
      } shouldBe e
    }
  }

  "runs cancel token" {
    checkAll(Arb.int()) {
      val latch = Promise<Unit>()
      var effect = 0

      val s = Stream.cancellableCallbackStream<Int> {
        CancelToken { effect += 1 }
      }

      val f = ForkAndForget {
        parTupledN(
          { s.compile().drain() },
          { latch.complete(Unit) }
        )
      }

      parTupledN({ latch.get() }, { sleep(50.milliseconds) })

      f.cancel()

      effect shouldBe 1
    }
  }

  "doesn't run cancel token without cancellation" {
    var effect = 0

    Stream.cancellableCallbackStream<Int> {
      end()
      CancelToken { effect += 1 }
    }
      .compile()
      .drain()

    effect shouldBe 0
  }

})

private fun <A> countToCallback(
  iterations: Int,
  map: (Int) -> A,
  cb: suspend (A) -> Unit
): Unit = suspend {
  var i = 0
  arrow.fx.coroutines.repeat(Schedule.recurs(iterations)) {
    i += 1
    cb(map(i))
  }
}.startCoroutine(Continuation(EmptyCoroutineContext) { })