package arrow.fx.coroutines

import kotlin.coroutines.Continuation
import kotlin.coroutines.intrinsics.*
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.startCoroutine

/**
 * A suspending function that forces to suspend,
 * and moves the stack to the heap.
 *
 * Using this function an allow for preventing [StackOverflowError] for
 * functions that cannot be written in a `tailrec` fashion.
 */
suspend fun trampoline(): Unit =
  suspendCoroutineUninterceptedOrReturn { cont ->
    // `startCoroutine` ensures intercepting on [Platform.trampoline]
    suspend { Unit }.startCoroutine(Continuation(Platform.trampoline()) {
      it.fold(
        { cont.resumeWith(Result.success(Unit)) },
        { e -> cont.resumeWithException(e) }
      )
    })

    COROUTINE_SUSPENDED
  }
