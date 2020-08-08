package arrow.benchmarks

import arrow.core.Either
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
open class AttemptNonRaised {

  @Param("10000")
  var size: Int = 0

  private fun ioLoopHappy(size: Int, i: Int): IO<Int> =
    if (i < size) {
      IO { i + 1 }.attempt().flatMap {
        it.fold(IO.Companion::raiseError) { n -> ioLoopHappy(size, n) }
      }
    } else IO.just(1)

  tailrec suspend fun loopHappy(size: Int, i: Int): Int =
    if (i < size) {
      val x = try {
        Either.Right(i + 1)
      } catch (e: Throwable) {
        Either.Left(e)
      }

      when (x) {
        is Either.Left -> throw x.a
        is Either.Right -> loopHappy(size, x.b)
      }
    } else 1


  @Benchmark
  fun legacy(): Int =
    ioLoopHappy(size, 0).unsafeRunSync()

  @Benchmark
  fun fx(): Int =
    env.unsafeRunSync {
      loopHappy(size, 0)
    }

  @Benchmark
  fun cats(): Any =
    arrow.benchmarks.effects.scala.cats.AttemptNonRaised.ioLoopHappy(size, 0).unsafeRunSync()

  @Benchmark
  fun zio(): Any =
    arrow.benchmarks.effects.scala.zio.AttemptNonRaised.run(size)

  @Benchmark
  fun kio(): Int =
    arrow.benchmarks.effects.kio.AttemptNonRaised.attemptNonRaised(size)
}
