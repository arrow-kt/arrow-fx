package arrow.benchmarks.effects.scala.zio

import zio._

object AttemptNonRaised {

  def ioLoopHappy(size: Int, i: Int): Task[Int] =
    if (i < size) {
      IO.effect {
        i + 1
      }.either.flatMap { result =>
          result.fold[Task[Int]](e => Task.fail(e), n => ioLoopHappy(size, n))
        }
    } else IO.succeed(1)

  def run(size: Int) = Runtime.default.unsafeRun(ioLoopHappy(size, 0))

}
