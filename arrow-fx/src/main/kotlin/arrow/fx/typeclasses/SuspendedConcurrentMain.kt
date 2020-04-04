package arrow.fx.typeclasses

import arrow.fx.IO

object SuspendedConcurrentMain {

  object File {
    suspend fun open(): File = this
    suspend fun use(): String = throw RuntimeException("BOOM")
    suspend fun release(): Unit = println("File closed")
  }

  @JvmStatic
  suspend fun main(args: Array<String>) {
    IO.fx {
      val lines = eff { File.open() }.bracketCase({
        release()
        println("released with case : $it")
      }, {
        use()
      })
      eff { println("found lines: $lines") }
    }
  }
}



