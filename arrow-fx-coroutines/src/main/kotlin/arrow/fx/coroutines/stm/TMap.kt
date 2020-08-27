package arrow.fx.coroutines.stm

import arrow.core.Tuple2
import arrow.core.toMap
import arrow.fx.coroutines.STM
import arrow.fx.coroutines.newTVar

suspend fun <K, V> STM.newTMap(): TMap<K, V> = TMap(newTVar(emptyMap()))
suspend fun <K, V> STM.newTMap(vararg arr: Pair<K, V>): TMap<K, V> =
  TMap(newTVar(arr.toMap().mapValues { newTVar(it.value) }))
suspend fun <K, V> STM.newTMap(vararg arr: Tuple2<K, V>): TMap<K, V> =
  TMap(newTVar(arr.toMap().mapValues { newTVar(it.value) }))
suspend fun <K, V> STM.newTMap(xs: Iterable<Pair<K, V>>): TMap<K, V> =
  TMap(newTVar(xs.toMap().mapValues { newTVar(it.value) }))
@JvmName("newFromIterableTuple")
suspend fun <K, V> STM.newTMap(xs: Iterable<Tuple2<K, V>>): TMap<K, V> =
  TMap(newTVar(xs.toMap().mapValues { newTVar(it.value) }))

/**
 * This encoding is not ideal for structural changes, better would be a map that based on a tree
 *  implementation to only invalidate the search path on inserts/deletes.
 */
data class TMap<K, V>internal constructor(internal val v: TVar<Map<K, TVar<V>>>) {
  companion object {
    suspend fun <K, V> new(): TMap<K, V> = TMap(TVar.new(emptyMap()))
    suspend fun <K, V> new(vararg arr: Pair<K, V>): TMap<K, V> =
      TMap(TVar.new(arr.toMap().mapValues { TVar.new(it.value) }))
    suspend fun <K, V> new(vararg arr: Tuple2<K, V>): TMap<K, V> =
      TMap(TVar.new(arr.toMap().mapValues { TVar.new(it.value) }))
    suspend fun <K, V> new(xs: Iterable<Pair<K, V>>): TMap<K, V> =
      TMap(TVar.new(xs.toMap().mapValues { TVar.new(it.value) }))
    @JvmName("newFromIterableTuple")
    suspend fun <K, V> new(xs: Iterable<Tuple2<K, V>>): TMap<K, V> =
      TMap(TVar.new(xs.toMap().mapValues { TVar.new(it.value) }))
  }
}
