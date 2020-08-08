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
import arrow.fx.extensions.io.applicativeError.handleErrorWith as ioHandleError

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5)
@CompilerControl(CompilerControl.Mode.DONT_INLINE)
open class HandleRaisedError {

  @Param("10000")
  var size: Int = 0

  private val dummy = RuntimeException("dummy")

  private fun ioErrorRaisedloop(i: Int): IO<Int> =
    if (i < size)
      IO.raiseError<Int>(dummy)
        .flatMap { x -> IO.just(x + 1) }
        .ioHandleError { ioErrorRaisedloop(i + 1) }
    else IO.just(i)

  tailrec suspend fun errorRaisedLoop(i: Int): Int =
    if (i < size) {
      val ii = try {
        throw dummy
        i + 1
      } catch (e: Throwable) {
        i + 1
      }
      errorRaisedLoop(ii)
    } else i

  @Benchmark
  fun io(): Int =
    ioErrorRaisedloop(0).unsafeRunSync()

  @Benchmark
  fun fx(): Int =
    env.unsafeRunSync { errorRaisedLoop(0) }
}
