package arrow.fx.coroutines.stream.test

import arrow.fx.coroutines.Environment
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.concurrent.Queue
import arrow.fx.coroutines.stream.drain
import arrow.fx.coroutines.stream.firstOrNull
import arrow.fx.coroutines.stream.handleErrorWith
import arrow.fx.coroutines.stream.toList
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime
import kotlin.time.toDuration
import arrow.fx.coroutines.Duration as ArrowDuration

@ExperimentalTime
fun testStream(
  timeout: Duration = 1.toDuration(DurationUnit.SECONDS),
  block: suspend TestStream.() -> Unit
): Unit = testStreamCompat(timeout = timeout.toArrowDuration(), block = block)

fun testStreamCompat(
  timeout: ArrowDuration = ArrowDuration(1, TimeUnit.SECONDS),
  block: suspend TestStream.() -> Unit
): Unit = Environment().unsafeRunSync { TestStream(timeout = timeout).block() }

// TODO: Convert to Kotlin Duration when not experimental
class TestStream internal constructor(private val timeout: ArrowDuration) {

  fun Stream<*>.capture() {
    Environment().unsafeRunAsync {
      through(queue.enqueue())
        .handleErrorWith { Stream.effect { exceptionQueue.enqueue1(it) } }
        .drain()
    }
  }

  suspend inline fun expect(item: Any?): Unit = next().let { next ->
    check(next == item) { "Expected $item but got $next" }
  }

  suspend inline fun expect(vararg items: Any?): Unit = next(items.size).let { next ->
    check(next.contentEquals(items)) {
      val itemsOut = items.joinToString(", ", "[", "]")
      val nextOut = next.joinToString(", ", "[", "]")
      "Expected $itemsOut but got $nextOut"
    }
  }

  suspend inline fun expectException(exception: Throwable): Unit = nextException().let { next ->
    check(next == exception) { "Expected $exception but got $next" }
  }

  suspend fun expectNothingMore() {
    val option = queue.tryDequeue1()
    check(option.isEmpty()) { "Expected nothing more but got: $option" }
  }

  suspend fun next(): Any? = queue.next()

  suspend fun nextException(): Any? = exceptionQueue.next()

  suspend fun next(n: Int): Array<Any?> =
    queue.dequeue().timeout(timeout).take(n).toList().toTypedArray()

  private val queue = Queue.unsafeUnbounded<Any?>()
  private val exceptionQueue = Queue.unsafeUnbounded<Throwable>()

  private suspend fun <O> Queue<O>.next(): O =
    dequeue().interruptAfter(timeout).firstOrError { "Timeout after $timeout" }
}

private suspend fun <O> Stream<O>.firstOrError(message: () -> String) =
  firstOrNull() ?: error(message())

@ExperimentalTime
private fun Duration.toArrowDuration(): ArrowDuration =
  ArrowDuration(toLongMilliseconds(), TimeUnit.MILLISECONDS)
