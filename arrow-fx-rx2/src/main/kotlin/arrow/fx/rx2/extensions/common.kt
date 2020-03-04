package arrow.fx.rx2.extensions

import arrow.core.Tuple2
import arrow.core.Tuple3
import io.reactivex.functions.BiFunction
import io.reactivex.functions.Function3

internal fun <A, B, C> ((Tuple2<A, B>) -> C).toBiFunction() =
  BiFunction { a: A, b: B -> this(Tuple2(a, b)) }

internal fun <A, B, C, D> ((Tuple3<A, B, C>) -> D).toFunction3() =
  Function3 { a: A, b: B, c: C -> this(Tuple3(a, b, c)) }
