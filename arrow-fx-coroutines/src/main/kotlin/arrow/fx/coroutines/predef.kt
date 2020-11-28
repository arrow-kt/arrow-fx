package arrow.fx.coroutines

import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.startCoroutineUninterceptedOrReturn
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

internal inline infix fun <A, B, C> ((A) -> B).andThen(crossinline f: (B) -> C): (A) -> C =
  { a -> f(this(a)) }

internal inline infix fun <A, B, C> (suspend (A) -> B).andThen(crossinline f: suspend (B) -> C): suspend (A) -> C =
  { a: A -> f(this(a)) }

@PublishedApi
internal infix fun <A> A.prependTo(fa: Iterable<A>): List<A> =
  listOf(this) + fa

internal fun <A> Iterable<A>.deleteFirst(f: (A) -> Boolean): Pair<A, List<A>>? {
  tailrec fun go(rem: Iterable<A>, acc: List<A>): Pair<A, List<A>>? =
    when {
      rem.isEmpty() -> null
      else -> {
        val a = rem.first()
        val tail = rem.drop(1)
        if (!f(a)) go(tail, acc + a)
        else Pair(a, acc + tail)
      }
    }

  return go(this, emptyList())
}

internal fun <A> Iterable<A>.uncons(): Pair<A, List<A>>? =
  firstOrNull()?.let { Pair(it, drop(1)) }

internal fun Iterable<*>.isEmpty(): Boolean =
  size() == 0

internal fun Iterable<*>.size(): Int =
  when (this) {
    is Collection -> size
    else -> fold(0) { acc, _ -> acc + 1 }
  }

/** Represents a unique identifier using object equality. */
internal class Token {
  override fun toString(): String = "Token(${Integer.toHexString(hashCode())})"
}

/**
 * Use this function instead of [suspendCoroutine] when you always suspend, but don't always need to `intercept` before returning.
 * It's like [suspendCoroutine] without the creation of `SafeContinuation` or `intercepted` upon returning.
 */
internal suspend inline fun <A> suspendCoroutineUnintercepted(crossinline f: (Continuation<A>) -> Unit): A =
  suspendCoroutineUninterceptedOrReturn {
    f(it)
    COROUTINE_SUSPENDED
  }

/**
 * Use this function to restart a coroutine directly from inside of [suspendCoroutine],
 * when the code is already in the context of this coroutine.
 * It does not use [ContinuationInterceptor] and does not update the context of the current thread.
 */
internal fun <A> (suspend () -> A).startCoroutineUnintercepted(completion: Continuation<A>): Unit =
  startDirect(completion) { actualCompletion ->
    startCoroutineUninterceptedOrReturn(actualCompletion)
  }

internal fun <R, A> (suspend R.() -> A).startCoroutineUnintercepted(receiver: R, completion: Continuation<A>): Unit =
  startDirect(completion) { actualCompletion ->
    startCoroutineUninterceptedOrReturn(receiver, actualCompletion)
  }

internal fun <A> (suspend () -> A).startCoroutineUninterceptedOrReturn(completion: Continuation<A>): Any? =
  startCoroutineUninterceptedOrReturn(probeCoroutineCreated(completion))

internal fun <R, A> (suspend R.() -> A).startCoroutineUninterceptedOrReturn(receiver: R, completion: Continuation<A>): Any? =
    startCoroutineUninterceptedOrReturn(receiver, probeCoroutineCreated(completion))

/**
 * Starts the given [block] immediately in the current stack-frame until the first suspension point.
 * This method supports debug probes and thus can intercept completion, thus completion is provided
 * as the parameter of [block].
 */
private inline fun <T> startDirect(completion: Continuation<T>, block: (Continuation<T>) -> Any?): Unit {
  val actualCompletion = probeCoroutineCreated(completion)
  val value = try {
    block(actualCompletion)
  } catch (e: Throwable) {
    actualCompletion.resumeWithException(e)
    return
  }
  if (value !== COROUTINE_SUSPENDED) {
    @Suppress("UNCHECKED_CAST")
    actualCompletion.resume(value as T)
  }
}
