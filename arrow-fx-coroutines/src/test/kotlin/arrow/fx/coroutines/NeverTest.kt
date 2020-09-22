package arrow.fx.coroutines

import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf

class NeverTest : ArrowFxSpec(spec = {
  "never throws CancellationException on cancel" {
    val latch = Promise<CancellationException>()

    val fiber = ForkAndForget {
      try {
        never<Unit>()
      } catch (cancelled: CancellationException) {
        latch.complete(cancelled)
      }
    }

    sleep(1.seconds)

    fiber.cancel()

    latch.get().shouldBeInstanceOf<CancellationException>()
  }

  "never runs finally on cancellation" {
    val latch = Promise<CancellationException>()
    val finished = Promise<Unit>()

    val fiber = ForkAndForget {
      try {
        never<Unit>()
      } catch (cancelled: CancellationException) {
        latch.complete(cancelled)
      } finally {
        finished.complete(Unit)
      }
    }

    sleep(1.seconds)

    fiber.cancel()

    latch.get().shouldBeInstanceOf<CancellationException>()
    finished.get() shouldBe Unit
  }
})
