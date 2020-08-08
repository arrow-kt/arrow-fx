package arrow.benchmarks.effects.scala.zio

import zio._

object Delay {

  def ioDelayLoop(size: Int, i: Int): Task[Int] =
    ZIO.effect { i } .flatMap { j =>
      if (j > size) ZIO.effect { j } else ioDelayLoop(size, j + 1)
    }

  def unsafeIODelayLoop(size: Int, i: Int): Int =
    Runtime.default.unsafeRun(ioDelayLoop(size, i))

}
