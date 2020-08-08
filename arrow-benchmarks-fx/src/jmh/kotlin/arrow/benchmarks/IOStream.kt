package arrow.benchmarks

import arrow.core.None
import arrow.core.Option
import arrow.core.Some
import arrow.fx.IO
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.CompilerControl
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Warmup
import java.util.concurrent.TimeUnit

object IOStream {
  class Stream(val value: Int, val next: IO<Option<Stream>>)

  private val addOne: (Int) -> Int = { it + 1 }

  fun test(times: Int, batchSize: Int): IO<Long> {
    var stream = range(0, times)
    var i = 0
    while (i < batchSize) {
      stream = mapStream(addOne, stream)
      i += 1
    }

    return sum(0, stream)
  }

  private fun range(from: Int, until: Int): Option<Stream> =
    if (from < until) Some(Stream(from, IO { range(from + 1, until) }))
    else None

  private fun mapStream(f: (Int) -> Int, box: Option<Stream>): Option<Stream> =
    when (box) {
      is Some -> box.copy(Stream(f(box.t.value), box.t.next.map { mapStream(f, it) }))
      None -> None
    }

  private fun sum(acc: Long, box: Option<Stream>): IO<Long> =
    when (box) {
      is Some -> box.t.next.flatMap { sum(acc + box.t.value, it) }
      None -> IO.just(acc)
    }
}

object SuspendStream {
  class Stream(val value: Int, val next: suspend () -> Stream?)

  private val addOne: (Int) -> Int = { it + 1 }

  suspend fun test(times: Int, batchSize: Int): Long {
    var stream = range(0, times)
    var i = 0

    while (i < batchSize) {
      stream = mapStream(addOne, stream)
      i += 1
    }

    return sum(0, stream)
  }

  private fun range(from: Int, until: Int): Stream? =
    if (from < until) Stream(from, suspend { range(from + 1, until) })
    else null

  private fun mapStream(f: (Int) -> Int, box: Stream?): Stream? =
    if (box != null) Stream(f(box.value), suspend { mapStream(f, box.next.invoke()) })
    else null

  private tailrec suspend fun sum(acc: Long, box: Stream?): Long =
    if (box != null) sum(acc + box.value, box.next.invoke())
    else acc
}
