package arrow.benchmarks

import arrow.fx.IO
import arrow.fx.coroutines.bracket
import arrow.unsafe
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.CompilerControl
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Warmup
import java.util.concurrent.TimeUnit
import arrow.fx.extensions.io.unsafeRun.runBlocking as ioRunBlocking

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5)
@CompilerControl(CompilerControl.Mode.DONT_INLINE)
open class Bracket {

  @Param("100")
  var size: Int = 0

  private fun ioBracketLoop(i: Int): IO<Int> =
    if (i < size)
      IO.just(i).bracket({ IO.unit }, { ib -> IO { ib + 1 } }).flatMap { ioBracketLoop(it) }
    else
      IO.just(i)

  tailrec suspend fun bracketLoop(i: Int): Int =
    if (i < size) {
      val next = bracket(acquire = { i }, use = { ii -> ii + 1 }, release = { Unit })
      bracketLoop(next)
    } else i

  @Benchmark
  fun io() = unsafe {
    ioRunBlocking {
      ioBracketLoop(0)
    }
  }

  @Benchmark
  fun fx() = env.unsafeRunSync {
    bracketLoop(0)
  }

}
