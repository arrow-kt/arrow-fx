package arrow.benchmarks

import arrow.fx.IO
import arrow.fx.IODispatchers
import arrow.fx.coroutines.parMapN
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
open class ParMap {

  @Param("100")
  var size: Int = 0

  private fun ioHelper(): IO<Int> =
    (0 until size).fold(IO { 0 }) { acc, i ->
      IO.parMapN(IODispatchers.CommonPool, acc, IO { i }) { (a, b) -> a + b }
    }

  suspend fun helper(): Int =
    (0 until size).fold(suspend { 0 }) { acc, i ->
      suspend { parMapN({ acc.invoke() }, { i }) { (a, b) -> a + b } }
    }.invoke()

  @Benchmark
  fun io(): Int =
    ioHelper().unsafeRunSync()

  @Benchmark
  fun fx(): Int =
    env.unsafeRunSync { helper() }
}
