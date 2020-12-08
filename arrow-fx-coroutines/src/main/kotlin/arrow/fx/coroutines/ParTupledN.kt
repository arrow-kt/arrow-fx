package arrow.fx.coroutines

import kotlinx.coroutines.Dispatchers
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

suspend fun <A, B> parTupledN(fa: suspend () -> A, fb: suspend () -> B): Pair<A, B> =
  parTupledN(Dispatchers.Default, fa, fb)

suspend fun <A, B, C> parTupledN(fa: suspend () -> A, fb: suspend () -> B, fc: suspend () -> C): Triple<A, B, C> =
  parTupledN(Dispatchers.Default, fa, fb, fc)

/**
 * Tuples [fa], [fb] on the provided [CoroutineContext].
 * If the context does not have any dispatcher nor any other [ContinuationInterceptor], then [Dispatchers.Default] is used.
 * Cancelling this operation cancels both tasks running in parallel.
 */
suspend fun <A, B> parTupledN(
  ctx: CoroutineContext = EmptyCoroutineContext,
  fa: suspend () -> A,
  fb: suspend () -> B
): Pair<A, B> =
  parMapN(ctx, fa, fb, ::Pair)

/**
 * Tuples [fa], [fb] & [fc] on the provided [CoroutineContext].
 * If the context does not have any dispatcher nor any other [ContinuationInterceptor], then [Dispatchers.Default] is used.
 * Cancelling this operation cancels both tasks running in parallel.
 */
suspend fun <A, B, C> parTupledN(
  ctx: CoroutineContext = EmptyCoroutineContext,
  fa: suspend () -> A,
  fb: suspend () -> B,
  fc: suspend () -> C
): Triple<A, B, C> =
  parMapN(ctx, fa, fb, fc, ::Triple)
