package arrow.fx.coroutines

inline infix fun <A, B, C> ((A) -> B).andThen(crossinline f: (B) -> C): (A) -> C =
  { a -> f(this(a)) }

inline infix fun <A, B, C> (suspend (A) -> B).andThen(crossinline f: suspend (B) -> C): suspend (A) -> C =
  { a: A -> f(this(a)) }

internal fun Iterable<*>.size(): Int =
  when (this) {
    is Collection -> size
    else -> fold(0) { acc, _ -> acc + 1 }
  }

/** Represents a unique identifier using object equality. */
internal class Token {
  override fun toString(): String = "Token(${Integer.toHexString(hashCode())})"
}
