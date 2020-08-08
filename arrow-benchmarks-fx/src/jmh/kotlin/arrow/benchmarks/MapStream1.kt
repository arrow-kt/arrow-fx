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
open class MapStream1 {

  @Benchmark
  fun legacy(): Long = IOStream.test(12000, 1).unsafeRunSync()

  @Benchmark
  fun fx(): Long =
    env.unsafeRunSync { SuspendStream.test(12000, 1) }

  @Benchmark
  fun zio(): Long =
    arrow.benchmarks.effects.scala.zio.`MapStream$`.`MODULE$`.test(12000, 1)

  @Benchmark
  fun cats(): Long =
    arrow.benchmarks.effects.scala.cats.`MapStream$`.`MODULE$`.test(12000, 1)

  @Benchmark
  fun kio(): Long =
    arrow.benchmarks.effects.kio.MapStream.test(12000, 1)

}
