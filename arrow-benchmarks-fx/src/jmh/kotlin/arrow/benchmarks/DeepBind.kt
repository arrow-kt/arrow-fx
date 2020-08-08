package arrow.benchmarks

import arrow.fx.IO
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
open class DeepBind {

  @Param("20")
  var depth: Int = 0

  fun ioFibLazy(n: Int): IO<Int> =
    if (n <= 1) IO { n }
    else ioFibLazy(n - 1).flatMap { a ->
      ioFibLazy(n - 2).flatMap { b -> IO { a + b } }
    }

  suspend fun fibLazy(n: Int, prev: Int = 0, next: Int = 1): Int =
    when (n) {
      0 -> prev
      1 -> next
      else -> fibLazy(n - 1, next, prev + next)
    }

  @Benchmark
  fun io(): Int =
    ioFibLazy(depth).unsafeRunSync()

  @Benchmark
  fun fx(): Int =
    env.unsafeRunSync {
      fibLazy(depth)
    }

  @Benchmark
  fun cats(): Any =
    arrow.benchmarks.effects.scala.cats.`DeepBind$`.`MODULE$`.fib(depth).unsafeRunSync()

  @Benchmark
  fun zio(): Any =
    arrow.benchmarks.effects.scala.zio.`DeepBind$`.`MODULE$`.fib(depth)

  @Benchmark
  fun kio(): Any =
    arrow.benchmarks.effects.kio.DeepBind.fib(depth)
}
