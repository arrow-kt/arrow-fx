package arrow.fx.coroutines

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.delay
import kotlin.coroutines.coroutineContext
import kotlin.time.seconds

class CancelBoundary : StringSpec({

  suspend fun forever(): Unit {
    while (true) {
      coroutineContext.ensureActive() // cancellable computation loop
    }
  }

  "endless loop can be cancelled if it includes a boundary" {
    val latch = Promise<Unit>()
    val exit = Promise<ExitCase>()
    val f = ForkConnected {
      guaranteeCase({
        latch.complete(Unit)
        forever()
      }, { ec -> exit.complete(ec) })
    }
    latch.get()
    f.cancel()
    exit.get().shouldBeInstanceOf<ExitCase.Cancelled>()
    delay(1.seconds)
  }
})
