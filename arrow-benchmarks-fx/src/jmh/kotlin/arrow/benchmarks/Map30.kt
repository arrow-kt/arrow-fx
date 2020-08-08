package arrow.benchmarks

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.CompilerControl
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Warmup
import java.util.concurrent.TimeUnit

@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5)
@CompilerControl(CompilerControl.Mode.DONT_INLINE)
open class Map30 {

  @Benchmark
  fun zio(): Long =
    arrow.benchmarks.effects.scala.zio.`Map$`.`MODULE$`.zioMapTest(12000 / 30, 30)

  @Benchmark
  fun cats(): Long =
    arrow.benchmarks.effects.scala.cats.`Map$`.`MODULE$`.catsIOMapTest(12000 / 30, 30)

  @Benchmark
  fun legacy(): Long = ioTest(12000 / 30, 30)

  @Benchmark
  fun kio(): Long =
    arrow.benchmarks.effects.kio.Map.kioMapTest(12000 / 30, 30)

  @Benchmark
  fun fx(): Long = fxTest(12000 / 30, 30)
}
