@file:Suppress("DEPRECATION_ERROR")

package arrow.fx.coroutines

import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeoutOrNull

/**
 * Sleeps for a given [duration] without blocking a thread.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.coroutines.*
 *
 * suspend fun main(): Unit {
 *   sleep(5.seconds)
 *   println("Message after sleeping")
 * }
 * ```
 **/
@Deprecated("Use delay", ReplaceWith("delay(duration.millis)", "kotlinx.coroutines.delay"), DeprecationLevel.ERROR)
suspend fun sleep(duration: Duration): Unit =
  delay(duration.millis)

/**
 * Returns the result of [fa] within the specified [duration] or returns null.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.coroutines.*
 *
 * suspend fun main(): Unit {
 *   timeOutOrNull(2.seconds) {
 *     sleep(5.seconds)
 *     "Message from lazy task"
 *   }.also(::println)
 *
 *   timeOutOrNull(2.seconds) {
 *     "Message from fast task"
 *   }.also(::println)
 * }
 * ```
 **/
@Deprecated(
  "use withTimeOutOrNull or timeOutOrNull(kotlin.time.Duration, fa)",
  ReplaceWith("withTimeoutOrNull(duration.millis)", "kotlinx.coroutines.withTimeoutOrNull")
)
suspend fun <A> timeOutOrNull(duration: Duration, fa: suspend () -> A): A? =
  withTimeoutOrNull(duration.millis) { fa.invoke() }

/**
 * Convenience function that returns the result of [fa] within the specified [duration] or returns null.
 *
 * ```kotlin:ank:playground
 * import kotlin.time.seconds
 * import kotlinx.coroutines.delay
 * import arrow.fx.coroutines.*
 *
 * suspend fun main(): Unit {
 *   timeOutOrNull(2.seconds) {
 *     delay(5.seconds)
 *     "Message from lazy task"
 *   }.also(::println)
 *
 *   timeOutOrNull(2.seconds) {
 *     "Message from fast task"
 *   }.also(::println)
 * }
 * ```
 **/
suspend fun <A> timeOutOrNull(duration: kotlin.time.Duration, fa: suspend () -> A): A? =
  withTimeoutOrNull(duration) { fa.invoke() }
