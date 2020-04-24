package arrow.fx.reactor.extensions

import arrow.core.Tuple2
import java.util.function.BiFunction

fun <A, B> tupled(): BiFunction<A, B, Tuple2<A, B>> =
  BiFunction<A, B, Tuple2<A, B>> { t, u -> Tuple2(t, u) }
