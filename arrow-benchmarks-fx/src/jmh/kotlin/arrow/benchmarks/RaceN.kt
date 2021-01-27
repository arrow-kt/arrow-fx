package arrow.benchmarks

import arrow.core.Either
import arrow.core.merge
import arrow.fx.coroutines.raceN
import kotlinx.coroutines.runBlocking
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
open class RaceN {

  @Param("100")
  var size: Int = 0

  @Benchmark
  fun coroutines(): Int = runBlocking {
    (0 until size).fold(0) { acc, _ ->
      when(val x = raceN({ acc }, { 1 })) {
        is Either.Left -> x.a
        is Either.Right -> x.b
      }
    }
  }
}
