package arrow.fx.coroutines

import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn

/**
 * Checks for cancellation,
 * when cancellation occurs this coroutine will suspend indefinitely and never continue.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.coroutines.*
 *
 * //sampleStart
 * suspend fun forever(): Unit {
 *   while(true) {
 *     println("I am getting dizzy...")
 *     cancelBoundary() // cancellable computation loop
 *   }
 * }
 *
 * suspend fun main(): Unit {
 *   val fiber = ForkConnected {
 *     guaranteeCase({ forever() }) { exitCase ->
 *       println("forever finished with $exitCase")
 *     }
 *   }
 *   sleep(10.milliseconds)
 *   fiber.cancel()
 * }
 * ```
 */
suspend fun cancelBoundary(): Unit =
  suspendCoroutineUninterceptedOrReturn { cont ->
    if (cont.context.connection().isCancelled()) COROUTINE_SUSPENDED
    else Unit
  }
