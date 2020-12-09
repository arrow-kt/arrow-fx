package arrow.fx.coroutines

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.selects.select
import kotlin.coroutines.CoroutineContext

sealed class Race3<out A, out B, out C> {
  data class First<A>(val winner: A) : Race3<A, Nothing, Nothing>()
  data class Second<B>(val winner: B) : Race3<Nothing, B, Nothing>()
  data class Third<C>(val winner: C) : Race3<Nothing, Nothing, C>()

  inline fun <D> fold(
    ifA: (A) -> D,
    ifB: (B) -> D,
    ifC: (C) -> D
  ): D = when (this) {
    is First -> ifA(winner)
    is Second -> ifB(winner)
    is Third -> ifC(winner)
  }
}

/**
 * Races the participants [fa], [fb] & [fc] in parallel on the [ComputationPool].
 * The winner of the race cancels the other participants.
 * Cancelling the operation cancels all participants.
 *
 * @see raceN for the same function that can race on any [CoroutineContext].
 */
suspend fun <A, B, C> raceN(
  fa: suspend () -> A,
  fb: suspend () -> B,
  fc: suspend () -> C
): Race3<A, B, C> = raceN(ComputationPool, fa, fb, fc)

/**
 * Races the participants [fa], [fb] & [fc] on the provided [CoroutineContext].
 * The winner of the race cancels the other participants.
 * Cancelling the operation cancels all participants.
 *
 * **WARNING**: operations run in parallel depending on the capabilities of the provided [CoroutineContext].
 * We ensure they start in sequence so it's guaranteed to finish on a single threaded context.
 *
 * @see raceN for a function that ensures operations run in parallel on the [ComputationPool].
 */
suspend fun <A, B, C> raceN(
  ctx: CoroutineContext,
  fa: suspend () -> A,
  fb: suspend () -> B,
  fc: suspend () -> C
): Race3<A, B, C> =
  coroutineScope {
    val a = async(ctx) { fa() }
    val b = async(ctx) { fb() }
    val c = async(ctx) { fc() }
    select<Race3<A, B, C>> {
      a.onAwait.invoke { Race3.First(it) }
      b.onAwait.invoke { Race3.Second(it) }
      c.onAwait.invoke { Race3.Third(it) }
    }.also {
      when (it) {
        is Race3.First -> cancelAndCompose(b, c)
        is Race3.Second -> cancelAndCompose(a, c)
        is Race3.Third -> cancelAndCompose(a, b)
      }
    }
  }

private suspend fun cancelAndCompose(first: Deferred<*>, second: Deferred<*>): Unit {
  val e1 = try {
    first.cancelAndJoin()
    null
  } catch (e: Throwable) {
    e.nonFatalOrThrow()
  }
  val e2 = try {
    second.cancelAndJoin()
    null
  } catch (e: Throwable) {
    e.nonFatalOrThrow()
  }
  Platform.composeErrors(e1, e2)?.let { throw it }
}
