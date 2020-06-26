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
import arrow.fx.coroutines.stream.async
import arrow.fx.coroutines.stream.asyncCancellable
import arrow.fx.coroutines.stream.chunk
import arrow.fx.coroutines.stream.compile
import arrow.fx.coroutines.throwable
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.list
import kotlin.coroutines.Continuation
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.startCoroutine

class CallbackStreamTest : StreamSpec(iterations = 250, spec = {

  "callbackStream should be lazy" {
    checkAll(Arb.int()) {
      var effect = 0
      Stream.async<Int> {
        effect += 1
      }

      effect shouldBe 0
    }
  }

  "emits values" {
    checkAll(Arb.list(Arb.int())) { list ->
      Stream.async {
        list.forEach { emit(it) }
        end()
      }
        .compile()
        .toList() shouldBe list
    }
  }

  "emits varargs" {
    checkAll(Arb.list(Arb.int())) { list ->
      Stream.async {
        emit(*list.toTypedArray())
        end()
      }
        .compile()
        .toList() shouldBe list
    }
  }

  "emits iterable" {
    checkAll(Arb.list(Arb.int())) { list ->
      Stream.async<Int> {
        emit(list)
        end()
      }
        .compile()
        .toList() shouldBe list
    }
  }

  "emits chunks" {
    checkAll(Arb.chunk(Arb.int()), Arb.chunk(Arb.int())) { ch, ch2 ->
      Stream.async<Int> {
        emit(ch)
        emit(ch2)
        end()
      }
        .chunks()
        .compile()
        .toList() shouldBe listOf(ch, ch2)
    }
  }

  "long running emission" {
    Stream.async {
      ForkAndForget {
        countToCallback(4, { it }, { emit(it) }) { end() }
      }
    }
      .compile()
      .toList() shouldBe listOf(1, 2, 3, 4, 5)
  }

  "emits and completes" {
    Stream.async {
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
      val s = Stream.async<Int> {
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

      val s = Stream.asyncCancellable<Int> {
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

    Stream.asyncCancellable<Int> {
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
  cb: suspend (A) -> Unit,
  onEnd: suspend () -> Unit = { }
): Unit = suspend {
  var i = 0
  arrow.fx.coroutines.repeat(Schedule.recurs(iterations)) {
    i += 1
    cb(map(i))
    sleep(500.milliseconds)
  }
  onEnd()
}.startCoroutine(Continuation(EmptyCoroutineContext) { })
