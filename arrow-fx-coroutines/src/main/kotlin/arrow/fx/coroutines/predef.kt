package arrow.fx.coroutines

import kotlin.coroutines.Continuation
import kotlin.coroutines.intrinsics.createCoroutineUnintercepted
import kotlin.coroutines.resume

/**
 * This value is used a a surrogate `null` value when needed.
 * It should never leak to the outside world.
 */
@JvmField
// @SharedImmutable Kotlin Native
internal val NULL = Symbol("NULL")

/**
 * A symbol class that is used to define unique constants that are self-explanatory in debugger.
 * Stolen from KotlinX
 */
internal class Symbol(val symbol: String) {
  override fun toString(): String = symbol

  @Suppress("UNCHECKED_CAST", "NOTHING_TO_INLINE")
  inline fun <T> unbox(value: Any?): T = if (value === this) null as T else value as T
}

internal inline infix fun <A, B, C> ((A) -> B).andThen(crossinline f: (B) -> C): (A) -> C =
  { a -> f(this(a)) }

internal fun Iterable<*>.size(): Int =
  when (this) {
    is Collection -> size
    else -> fold(0) { acc, _ -> acc + 1 }
  }

internal fun <A> (suspend () -> A).startCoroutineUnintercepted(completion: Continuation<A>): Unit =
  createCoroutineUnintercepted(completion).resume(Unit)

/** Represents a unique identifier using object equality. */
internal class Token {
  override fun toString(): String = "Token(${Integer.toHexString(hashCode())})"
}
