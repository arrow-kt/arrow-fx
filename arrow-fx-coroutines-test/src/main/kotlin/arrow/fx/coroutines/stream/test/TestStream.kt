package arrow.fx.coroutines.stream.test

import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.concurrent.Queue
import arrow.fx.coroutines.stream.drain
import arrow.fx.coroutines.stream.firstOrNull
import arrow.fx.coroutines.stream.test.lang.runBlocking
import arrow.fx.coroutines.stream.toList
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime
import kotlin.time.toDuration
import arrow.fx.coroutines.Duration as ArrowDuration

@OptIn(ExperimentalTime::class)
fun testStream(
  timeout: Duration = 1.toDuration(DurationUnit.SECONDS),
  block: suspend TestStream.() -> Unit,
): Unit = TestStream(timeout = timeout).runBlocking { block() }

@OptIn(ExperimentalTime::class)
class TestStream(val timeout: Duration) {

  suspend fun Stream<*>.capture(): Unit = through(queue.enqueue()).drain()

  suspend inline fun expect(item: Any?): Unit = next().let { next ->
    check(next == item) { "Expected $item but got $next" }
  }

  suspend fun expectNothingMore() {
    val values = queue.dequeue().interruptAfter(now).toList()
    check(values.isEmpty()) { "Expected nothing more but got: $values" }
  }

  private val queue = Queue.unsafeUnbounded<Any?>()

  suspend fun next(): Any? =
    queue.dequeue().interruptAfter(timeout).firstOrError { "Timeout after $timeout" }

}

// TODO(pablisco): Maybe move to TerminalOps
private suspend fun <O> Stream<O>.firstOrError(message: () -> String) =
  firstOrNull() ?: error(message())

@OptIn(ExperimentalTime::class)
private val now = 0.toDuration(TimeUnit.SECONDS)

@OptIn(ExperimentalTime::class)
private fun <O> Stream<O>.interruptAfter(duration: Duration): Stream<O> =
  interruptAfter(duration.toArrowDuration())

@OptIn(ExperimentalTime::class)
private fun Duration.toArrowDuration(): ArrowDuration =
  ArrowDuration(toLongMilliseconds(), TimeUnit.MILLISECONDS)
