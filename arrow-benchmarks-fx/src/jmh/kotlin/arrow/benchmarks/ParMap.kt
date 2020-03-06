package arrow.benchmarks

import arrow.core.extensions.list.foldable.foldLeft
import arrow.fx.IO
import arrow.fx.IODispatchers

import arrow.fx.extensions.io.concurrent.parMapN
import arrow.fx.unsafeRunSync
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
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10)
@CompilerControl(CompilerControl.Mode.DONT_INLINE)
open class ParMap {

  @Param("100")
  var size: Int = 0

  private fun ioHelper(): IO<Nothing, Int> =
    (0 until size).toList().foldLeft(IO { 0 }) { acc, i ->
      IO.parMapN(IODispatchers.CommonPool, acc, IO { i }) { (a, b) -> a + b }
    }

  @Benchmark
  fun io(): Int =
    ioHelper().unsafeRunSync()
}
