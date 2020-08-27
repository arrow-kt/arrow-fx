package arrow.fx.coroutines.stm

import arrow.fx.coroutines.STM
import arrow.fx.coroutines.newTVar

suspend fun <A> STM.newTSet(): TSet<A> =
  TSet(newTVar(emptySet()))
suspend fun <A> STM.newTSet(vararg arr: A): TSet<A> =
  TSet(newTVar(arr.map { newTVar(it) }.toSet()))
suspend fun <A> STM.newTSet(xs: Iterable<A>): TSet<A> =
  TSet(newTVar(xs.map { newTVar(it) }.toSet()))

/**
 * Similar to TMap this is inefficient on structure updates
 */
data class TSet<A>internal constructor(internal val v: TVar<Set<TVar<A>>>) {
  companion object {
    suspend fun <A> new(): TSet<A> =
      TSet(TVar.new(emptySet()))
    suspend fun <A> new(vararg arr: A): TSet<A> =
      TSet(TVar.new(arr.map { TVar.new(it) }.toSet()))
    suspend fun <A> new(xs: Iterable<A>): TSet<A> =
      TSet(TVar.new(xs.map { TVar.new(it) }.toSet()))
  }
}
