package arrow.fx.coroutines.stream.test

import arrow.fx.coroutines.Environment
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.concurrent.Queue
import arrow.fx.coroutines.stream.drain
import arrow.fx.coroutines.stream.firstOrNull
import arrow.fx.coroutines.stream.flatten
import arrow.fx.coroutines.stream.handleErrorWith
import arrow.fx.coroutines.stream.toList
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import arrow.fx.coroutines.Duration as ArrowDuration

@ExperimentalTime
fun testStream(
  timeout: Duration,
  captureTimeout: Duration = timeout,
  f: suspend TestStreamScope.() -> Unit
): Unit = testStream(
  timeout = timeout.toArrowDuration(),
  captureTimeout = captureTimeout.toArrowDuration(),
  f = f
)

fun testStream(
  timeout: ArrowDuration = ArrowDuration(1, TimeUnit.SECONDS),
  captureTimeout: ArrowDuration = timeout,
  f: suspend TestStreamScope.() -> Unit
): Unit = Environment().unsafeRunSync {
  TestStreamScope(timeout = timeout, captureTimeout = captureTimeout).f()
}

/**
 * This scope defines operations to [capture] Streams and [expect] values to be emitted.
 */
class TestStreamScope internal constructor(
  private val timeout: ArrowDuration,
  private val captureTimeout: ArrowDuration,
) {

  /**
   * This non-blocking terminal operation will observe all the values coming from the
   * receiver [Stream], timing out after the provided [captureTimeout].
   *
   * This operation can be called at any point for multiple streams. However, the order
   * of capture cannot be guaranteed
   *
   * By default [captureTimeout] is the same as [timeout], which in turn is 1 second
   * unless provided.
   *
   * @see captureInOrder
   */
  fun Stream<*>.capture() {
    Environment().unsafeRunAsync { blockingCapture().drain() }
  }

  /**
   * Captures all the values emitted by each of the provided streams in order.
   *
   * @see capture
   */
  fun captureInOrder(vararg streams: Stream<*>) {
    Environment().unsafeRunAsync {
      Stream(*streams).effectMap { it.blockingCapture() }.flatten().drain()
    }
  }

  private fun Stream<*>.blockingCapture(): Stream<Unit> =
    through(queue.enqueue())
      .handleErrorWith { Stream.effect { exceptionQueue.enqueue1(it) } }
      .interruptAfter(captureTimeout)


  /**
   * This waits for the next value emitted and compare it to the [item] provided.
   *
   * Timeouts after the time defined by [timeout]
   */
  suspend fun expect(item: Any?): Unit = next().let { next ->
    check(next == item) { "Expected $item but got $next" }
  }

  /**
   * Similar to [expect] but allowing to validate the value with the provided [predicate].
   *
   * And optional [onError] function can be provided to define what error to throw if
   * the value doesn't pass the condition of the predicate provided.
   */
  suspend inline fun <reified T> expectThat(
    onError: (T) -> String = { error("Failed expected condition for $it") },
    predicate: (T) -> Boolean,
  ): Unit = next()
    .let { it as T ?: error("Expected $it to be of type ${T::class}") }
    .let { check(predicate(it)) { onError(it) } }

  /**
   * Checks that the provided [items] are emitted without taking order in consideration.
   *
   * @see expectInOrder
   */
  suspend fun expectAll(vararg items: Any?): Unit = next(items.size).let { next ->
    check(next.all { it in items }) {
      "Expected ${items.toPrettyString()} but got ${next.toPrettyString()}"
    }
  }

  /**
   * Checked that the provided [items] are emitted in the same order.
   *
   * @see expectAll
   */
  suspend fun expectInOrder(vararg items: Any?): Unit = next(items.size).let { next ->
    check(items.contentDeepEquals(next)) {
      "Expected ${items.toPrettyString()} but got ${next.toPrettyString()}"
    }
  }

  private fun Array<out Any?>.toPrettyString() =
    joinToString(separator = ", ", prefix = "[", postfix = "]")

  /**
   * Fails if the [expectedException] is not thrown.
   */
  suspend fun expectException(expectedException: Throwable): Unit = nextException().let { next ->
    check(next == expectedException) { "Expected $expectedException but got $next" }
  }

  /**
   * Fails if there is an extra item emitted.
   */
  suspend fun expectNothingMore() {
    val option = nextOrNull()
    check(option == null) { "Expected nothing more but got: $option" }
  }

  /**
   * Waits for the next value emitted to any of the captured streams.
   *
   * Ideal to use with custom assertion libraries
   */
  suspend fun next(): Any? = queue.next()

  /**
   * Gets next value without waiting, returning null if a value hasn't been emitted.
   */
  suspend fun nextOrNull(): Any? = queue.tryDequeue1().orNull()

  /**
   * Waits for the next error thrown in any of the captured streams.
   *
   * Ideal to use with custom assertion libraries
   */
  suspend fun nextException(): Any? = exceptionQueue.next()

  /**
   * Waits for the next [n] values emitted to any of the captured streams.
   *
   * Ideal to use with custom assertion libraries
   */
  suspend fun next(n: Int): Array<Any?> =
    queue.dequeue().take(n).toList().toTypedArray()

  private val queue = Queue.unsafeUnbounded<Any?>()
  private val exceptionQueue = Queue.unsafeUnbounded<Throwable>()

  private suspend fun <O> Queue<O>.next(): O =
    dequeue().interruptAfter(timeout).firstOrElse { error("Timeout after $timeout") }
}

private suspend fun <O> Stream<O>.firstOrElse(alternative: () -> O): O =
  firstOrNull() ?: alternative()

@ExperimentalTime
private fun Duration.toArrowDuration(): ArrowDuration =
  ArrowDuration(toLongMilliseconds(), TimeUnit.MILLISECONDS)
