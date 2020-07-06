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

val dummy = object : RuntimeException("dummy") {
  override fun fillInStackTrace(): Throwable =
    this
}

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10)
@CompilerControl(CompilerControl.Mode.DONT_INLINE)
open class AttemptRaisedError {

  @Param("10000")
  var size: Int = 0

  private fun ioLoopNotHappy(size: Int, i: Int): IO<Int> =
    if (i < size) {
      IO { throw dummy }.attempt().flatMap {
        it.fold({ ioLoopNotHappy(size, i + 1) }, IO.Companion::just)
      }
    } else IO.just(1)

  @Benchmark
  fun io(): Int =
    ioLoopNotHappy(size, 0).unsafeRunSync()

  @Benchmark
  fun cats(): Any =
    arrow.benchmarks.effects.scala.cats.AttemptRaisedError.ioLoopNotHappy(size, 0).unsafeRunSync()

  @Benchmark
  fun zio(): Any =
    arrow.benchmarks.effects.scala.zio.AttemptRaisedError.run(size)

  @Benchmark
  fun kio(): Any =
    arrow.benchmarks.effects.kio.AttemptRaisedError.attemptRaisedError(size)
}
