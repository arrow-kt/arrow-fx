package arrow.fx.coroutines.stm

import arrow.fx.coroutines.STM
import arrow.fx.coroutines.newTVar

suspend fun <A> STM.newTQueue(): TQueue<A> = TQueue(newTVar(emptyList()), newTVar(emptyList()))

data class TQueue<A> internal constructor(
  internal val reads: TVar<List<A>>,
  internal val writes: TVar<List<A>>
) {
  companion object {
    suspend fun <A> new(): TQueue<A> = TQueue(TVar.new(emptyList()), TVar.new(emptyList()))
  }
}
