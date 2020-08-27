package arrow.fx.coroutines.stm

import arrow.fx.coroutines.STM

suspend fun <A> STM.newTMVar(a: A): TMVar<A> = TMVar<A>(newTVar(a))
suspend fun <A> STM.newEmptyTMVar(): TMVar<A> = TMVar<A>(newTVar(null))

data class TMVar<A> internal constructor(internal val v: TVar<A?>) {
  companion object {
    suspend fun <A> new(a: A): TMVar<A> = TMVar<A>(TVar.new(a))
    suspend fun <A> empty(): TMVar<A> = TMVar<A>(TVar.new(null))
  }
}
