package arrow.fx.coroutines

import arrow.core.Either
import arrow.core.Left
import arrow.core.Right
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.selects.select
import kotlin.coroutines.Continuation
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlin.coroutines.intrinsics.intercepted
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED

/**
 * Races the participants [fa], [fb] in parallel on the [ComputationPool].
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
suspend fun <A, B> raceN(fa: suspend () -> A, fb: suspend () -> B): Either<A, B> =
  raceN(ComputationPool, fa, fb)

/**
 * Races the participants [fa], [fb] on the provided [CoroutineContext].
 * The winner of the race cancels the other participants.
 * Cancelling the operation cancels all participants.
 *
 * **WARNING**: operations run in parallel depending on the capabilities of the provided [CoroutineContext].
 * We ensure they start in sequence so it's guaranteed to finish on a single threaded context.
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
 * @see racePair for a version that does not automatically cancel the loser.
 * @see raceN for a function that ensures it runs in parallel on the [ComputationPool].
 */
suspend fun <A, B> raceN(ctx: CoroutineContext, fa: suspend () -> A, fb: suspend () -> B): Either<A, B> =
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
