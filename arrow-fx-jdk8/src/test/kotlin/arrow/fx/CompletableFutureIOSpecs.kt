package arrow.fx

import arrow.core.Either
import arrow.core.test.UnitSpec
import io.kotlintest.shouldBe
import java.util.concurrent.CompletableFuture

class CompletableFutureIOSpecs : UnitSpec() {

  init {

    "Success CompletableFuture results on success IO" {
      val future = CompletableFuture<String>()

      future.complete("yeah!")

      future.toIo().unsafeRunSyncEither() shouldBe Either.right("yeah!")
    }
  }
}
