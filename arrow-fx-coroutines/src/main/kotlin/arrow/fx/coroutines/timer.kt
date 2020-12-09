package arrow.fx.coroutines

import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService

/**
 * [scheduler] is **only** for internal use for the [sleep] implementation.
 * This way we can guarantee nothing besides sleeping ever occurs here.
 */
internal val scheduler: ScheduledExecutorService by lazy {
  Executors.newScheduledThreadPool(2) { r ->
    Thread(r).apply {
      name = "arrow-effect-scheduler-$id"
      isDaemon = true
    }
  }
}

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
@Deprecated("Use delay")
suspend fun sleep(duration: Duration): Unit =
  if (duration.amount <= 0) Unit
  else cancellable { resumeWith ->
    val cancelRef = scheduler.schedule(
      { resumeWith(Result.success(Unit)) },
      duration.amount,
      duration.timeUnit
    )

    CancelToken { cancelRef.cancel(false); Unit }
  }

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
@Deprecated("use withTimeOutOrNull")
suspend fun <A> timeOutOrNull(duration: Duration, fa: suspend () -> A): A? =
    withTimeoutOrNull(duration.millis) { fa.invoke() }
