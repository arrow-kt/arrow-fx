package arrow.fx.rx2.extensions

import arrow.core.Tuple2
import io.reactivex.functions.BiFunction

internal fun <A, B, C> ((Tuple2<A, B>) -> C).toBiFunction() = BiFunction { a: A, b: B -> this(Tuple2(a, b)) }
