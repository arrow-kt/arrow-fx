package arrow.fx.coroutines.stm

import arrow.fx.coroutines.ConcurrentVar
import arrow.fx.coroutines.STM

suspend fun <A> STM.newTMVar(a: A): TMVar<A> = TMVar<A>(newTVar(a))
suspend fun <A> STM.newEmptyTMVar(): TMVar<A> = TMVar<A>(newTVar(null))

/**
 * A [TMVar] is the [STM] analog to [ConcurrentVar].
 * It represents a reference that is either empty or full.
 *
 * The main use for [TMVar] is as a synchronization primitive as it can be used to force other transactions
 *  to wait until a [TMVar] is full.
 *
 * ## Creating a [TMVar]:
 *
 * As usual with [STM] types there are two equal sets of operators for creating them, one that can be used inside
 *  and one for use outside of transactions:
 * - [TMVar.new] and [STM.newTMVar] create a new filled [TMVar]
 * - [TMVar.empty] and [STM.newEmptyTMVar] create an empty [TMVar]
 *
 */
data class TMVar<A> internal constructor(internal val v: TVar<A?>) {
  companion object {
    suspend fun <A> new(a: A): TMVar<A> = TMVar<A>(TVar.new(a))
    suspend fun <A> empty(): TMVar<A> = TMVar<A>(TVar.new(null))
  }
}
