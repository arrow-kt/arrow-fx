package arrow.fx.coroutines.stm

import arrow.fx.coroutines.STM
import arrow.fx.coroutines.newTVar

suspend fun STM.newTSem(initial: Int): TSem = TSem(newTVar(initial))

data class TSem internal constructor(internal val v: TVar<Int>) {
  companion object {
    suspend fun new(initial: Int): TSem = TSem(TVar.new(initial))
  }
}
