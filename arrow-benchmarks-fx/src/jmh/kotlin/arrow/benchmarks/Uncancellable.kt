package arrow.benchmarks

import arrow.fx.IO
import arrow.fx.coroutines.uncancellable
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.CompilerControl
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Warmup
import java.util.concurrent.TimeUnit

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5)
@CompilerControl(CompilerControl.Mode.DONT_INLINE)
open class Uncancellable {

  @Param("100")
  var size: Int = 0

  fun ioUncancellableLoop(i: Int): IO<Int> =
    if (i < size) IO { i + 1 }.uncancellable().flatMap { ioUncancellableLoop(it) }
    else IO.just(i)

  tailrec suspend fun uncancellableLoop(i: Int): Int =
    if (i < size) {
      val x = uncancellable { i + 1 }
      uncancellableLoop(x + 1)
    } else i

  @Benchmark
  fun io(): Int = ioUncancellableLoop(0).unsafeRunSync()

  @Benchmark
  fun fx(): Int =
    env.unsafeRunSync { uncancellableLoop(0) }
}
