package arrow.fx.coroutines

import kotlinx.coroutines.Dispatchers
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

suspend inline fun <A, B> parTupledN(
  crossinline fa: suspend () -> A,
  crossinline fb: suspend () -> B
): Pair<A, B> =
  parTupledN(EmptyCoroutineContext, fa, fb)

suspend inline fun <A, B, C> parTupledN(
  crossinline fa: suspend () -> A,
  crossinline fb: suspend () -> B,
  crossinline fc: suspend () -> C
): Triple<A, B, C> =
  parTupledN(EmptyCoroutineContext, fa, fb, fc)

/**
 * Tuples [fa], [fb] on the provided [CoroutineContext].
 * If the context does not have any dispatcher nor any other [ContinuationInterceptor], then [Dispatchers.Default] is used.
 * Cancelling this operation cancels both tasks running in parallel.
 */
suspend inline fun <A, B> parTupledN(
  ctx: CoroutineContext = EmptyCoroutineContext,
  crossinline fa: suspend () -> A,
  crossinline fb: suspend () -> B
): Pair<A, B> =
  parMapN(ctx, fa, fb, ::Pair)

/**
 * Tuples [fa], [fb] & [fc] on the provided [CoroutineContext].
 * If the context does not have any dispatcher nor any other [ContinuationInterceptor], then [Dispatchers.Default] is used.
 * Cancelling this operation cancels both tasks running in parallel.
 */
suspend inline fun <A, B, C> parTupledN(
  ctx: CoroutineContext = EmptyCoroutineContext,
  crossinline fa: suspend () -> A,
  crossinline fb: suspend () -> B,
  crossinline fc: suspend () -> C
): Triple<A, B, C> =
  parMapN(ctx, fa, fb, fc, ::Triple)
