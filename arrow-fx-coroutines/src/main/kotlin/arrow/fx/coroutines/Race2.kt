package arrow.fx.coroutines

import arrow.core.Either
import arrow.core.Left
import arrow.core.Right
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.selects.select
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

/**
 * Races the participants [fa], [fb] in parallel on the [Dispatchers.Default].
 * The winner of the race cancels the other participants.
 * Cancelling the operation cancels all participants.
 * An [uncancellable] participant will back-pressure the result of [raceN].
 *
 * ```kotlin:ank:playground
 * import arrow.core.Either
 * import arrow.fx.coroutines.*
 *
 * suspend fun main(): Unit {
 *   suspend fun loser(): Int =
 *     cancellable { callback ->
 *        // Wait forever and never complete callback
 *        CancelToken { println("Never got cancelled for losing.") }
 *     }
 *
 *   val winner = raceN({ loser() }, { 5 })
 *
 *   val res = when(winner) {
 *     is Either.Left -> "Never always loses race"
 *     is Either.Right -> "Race was won with ${winner.b}"
 *   }
 *   //sampleEnd
 *   println(res)
 * }
 * ```
 *
 * @param fa task to participate in the race
 * @param fb task to participate in the race
 * @return either [Either.Left] if [fa] won the race, or [Either.Right] if [fb] won the race.
 * @see racePair for a version that does not automatically cancel the loser.
 * @see raceN for the same function that can race on any [CoroutineContext].
 */
suspend inline fun <A, B> raceN(crossinline fa: suspend () -> A, crossinline fb: suspend () -> B): Either<A, B> =
  raceN(EmptyCoroutineContext, fa, fb)

/**
 * Races the participants [fa], [fb] on the provided [CoroutineContext].
 * The winner of the race cancels the other participants.
 * Cancelling the operation cancels all participants.
 *
 * If the context does not have any dispatcher nor any other [ContinuationInterceptor], then [Dispatchers.Default] is used.
 *
 * ```kotlin:ank:playground
 * import arrow.core.Either
 * import arrow.fx.coroutines.*
 *
 * suspend fun main(): Unit {
 *   suspend fun loser(): Int =
 *     cancellable { callback ->
 *        // Wait forever and never complete callback
 *        CancelToken { println("Never got cancelled for losing.") }
 *     }
 *
 *   val winner = raceN(IOPool, { loser() }, { 5 })
 *
 *   val res = when(winner) {
 *     is Either.Left -> "Never always loses race"
 *     is Either.Right -> "Race was won with ${winner.b}"
 *   }
 *   //sampleEnd
 *   println(res)
 * }
 * ```
 *
 * @param fa task to participate in the race
 * @param fb task to participate in the race
 * @return either [Either.Left] if [fa] won the race, or [Either.Right] if [fb] won the race.
 * @see raceN for a function that ensures it runs in parallel on the [Dispatchers.Default].
 */
suspend inline fun <A, B> raceN(
  ctx: CoroutineContext = EmptyCoroutineContext,
  crossinline fa: suspend () -> A,
  crossinline fb: suspend () -> B
): Either<A, B> =
  coroutineScope {
    val a = async(ctx) { fa() }
    val b = async(ctx) { fb() }
    select<Either<A, B>> {
      a.onAwait.invoke { Left(it) }
      b.onAwait.invoke { Right(it) }
    }.also {
      when (it) {
        is Either.Left -> b.cancelAndJoin()
        is Either.Right -> a.cancelAndJoin()
      }
    }
  }
