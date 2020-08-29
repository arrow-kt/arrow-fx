package arrow.fx.coroutines.stm

import arrow.fx.coroutines.STM

suspend fun <A> STM.newTQueue(): TQueue<A> = TQueue(newTVar(emptyList()), newTVar(emptyList()))

/**
 * A [TQueue] is a transactional unbounded queue which can be written to and read from concurrently.
 *
 * ## Creating a [TQueue]
 *
 * Creating an empty queue can be done by using either [STM.newTQueue] or [TQueue.new] depending on whether or not you are in
 *  a transaction or not.
 *
 * ## Concurrent reading and writing
 *
 * [TQueue] is implemented using two [TVar]'s. One for reading and one for writing.
 * This effectively means that writing and reading accesses two different variables and thus it never blocks each other.
 *
 * > In practice reads have to access the writes variable if they run out of elements to read but this is infrequent.
 */
data class TQueue<A> internal constructor(
  internal val reads: TVar<List<A>>,
  internal val writes: TVar<List<A>>
) {
  companion object {
    suspend fun <A> new(): TQueue<A> = TQueue(TVar.new(emptyList()), TVar.new(emptyList()))
  }
}
