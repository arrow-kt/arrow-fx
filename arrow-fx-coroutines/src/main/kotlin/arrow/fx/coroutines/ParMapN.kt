package arrow.fx.coroutines

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

suspend fun <A, B, C> parMapN(
  fa: suspend () -> A,
  fb: suspend () -> B,
  f: (A, B) -> C
): C = parMapN(Dispatchers.Default, fa, fb, f)

/**
 * Runs [fa], [fb] in parallel on [ctx] and combines their results using the provided function.
 * If the context does not have any dispatcher nor any other [ContinuationInterceptor], then [Dispatchers.Default] is used.
 * Cancelling this operation cancels both operations running in parallel.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.coroutines.*
 *
 * suspend fun main(): Unit {
 *   //sampleStart
 *   val result = parMapN(
 *     { "First one is on ${Thread.currentThread().name}" },
 *     { "Second one is on ${Thread.currentThread().name}" }
 *   ) { a, b ->
 *       "$a\n$b"
 *     }
 *   //sampleEnd
 *  println(result)
 * }
 * ```
 *
 * @param fa value to parallel map
 * @param fb value to parallel map
 * @param f function to map/combine value [A] and [B]
 * ```
 *
 * @see parMapN for the same function that can race on any [CoroutineContext].
 */
suspend fun <A, B, C> parMapN(
  ctx: CoroutineContext = EmptyCoroutineContext,
  fa: suspend () -> A,
  fb: suspend () -> B,
  f: (A, B) -> C
): C {
  var a: Any? = NULL
  var b: Any? = NULL
  coroutineScope {
    launch(ctx) { a = fa() }
    launch(ctx) { b = fb() }
  }
  return f(NULL.unbox(a), NULL.unbox(b))
}

suspend fun <A, B, C, D> parMapN(
  fa: suspend () -> A,
  fb: suspend () -> B,
  fc: suspend () -> C,
  f: (A, B, C) -> D
): D = parMapN(Dispatchers.Default, fa, fb, fc, f)

/**
 * Runs [fa], [fb], [fc] in parallel on [ctx] and combines their results using the provided function.
 * If the context does not have any dispatcher nor any other [ContinuationInterceptor], then [Dispatchers.Default] is used.
 * Cancelling this operation cancels both operations running in parallel.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.coroutines.*
 *
 * suspend fun main(): Unit {
 *   //sampleStart
 *   val result = parMapN(
 *     { "First one is on ${Thread.currentThread().name}" },
 *     { "Second one is on ${Thread.currentThread().name}" },
 *     { "Thirf one is on ${Thread.currentThread().name}" }
 *   ) { a, b ->
 *       "$a\n$b\n$c"
 *     }
 *   //sampleEnd
 *  println(result)
 * }
 * ```
 *
 * @param fa value to parallel map
 * @param fb value to parallel map
 * @param fc value to parallel map
 * @param f function to map/combine value [A] and [B]
 * ```
 *
 * @see parMapN for the same function that can race on any [CoroutineContext].
 */
// TODO provide efficient impls like below for N-arity.
suspend fun <A, B, C, D> parMapN(
  ctx: CoroutineContext = EmptyCoroutineContext,
  fa: suspend () -> A,
  fb: suspend () -> B,
  fc: suspend () -> C,
  f: (A, B, C) -> D
): D = coroutineScope {
  val a = async(ctx) { fa() }
  val b = async(ctx) { fb() }
  val c = async(ctx) { fc() }
  f(a.await(), b.await(), c.await())
}
