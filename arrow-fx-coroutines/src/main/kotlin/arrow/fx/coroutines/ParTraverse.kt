package arrow.fx.coroutines

import arrow.core.Either
import arrow.core.Validated
import arrow.core.ap
import arrow.core.computations.either
import arrow.core.valid
import arrow.typeclasses.Semigroup
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.withPermit
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

suspend fun <A> Iterable<suspend () -> A>.parSequenceN(n: Long): List<A> =
  parSequenceN(Dispatchers.Default, n)

/**
 * Sequences all tasks in [n] parallel processes and return the result.
 * Cancelling this operation cancels all running tasks.
 *
 * **WARNING**: operations run in parallel depending on the capabilities of the provided [CoroutineContext].
 * We ensure they start in sequence so it's guaranteed to finish on a single threaded context.
 *
 * @see parSequence for a function that ensures it runs in parallel on the [ComputationPool].
 */
suspend fun <A> Iterable<suspend () -> A>.parSequenceN(ctx: CoroutineContext = EmptyCoroutineContext, n: Long): List<A> {
  val s = kotlinx.coroutines.sync.Semaphore(n.toInt())
  return parTraverse(ctx) {
    s.withPermit { it.invoke() }
  }
}

suspend fun <A> Iterable<suspend () -> A>.parSequence(): List<A> =
  parSequence(Dispatchers.Default)

/**
 * Sequences all tasks in parallel and return the result
 * Cancelling this operation cancels all running tasks.
 *
 * **WARNING**: operations run in parallel depending on the capabilities of the provided [CoroutineContext].
 * We ensure they start in sequence so it's guaranteed to finish on a single threaded context.
 *
 * @see parSequence for a function that ensures it runs in parallel on the [ComputationPool].
 */
suspend fun <A> Iterable<suspend () -> A>.parSequence(ctx: CoroutineContext = EmptyCoroutineContext): List<A> =
  parTraverse(ctx) { it.invoke() }

suspend fun <A, B> Iterable<A>.parTraverseN(n: Long, f: suspend (A) -> B): List<B> =
  parTraverseN(Dispatchers.Default, n, f)

/**
 * Traverses this [Iterable] and runs [f] in parallel on [ctx].
 * Cancelling this operation cancels all running tasks.
 *
 * **WARNING**: operations runs in parallel depending on the capabilities of the provided [CoroutineContext].
 * We ensure they start in sequence so it's guaranteed to finish on a single threaded context.
 *
 * @see parTraverseN for a function that ensures it runs in parallel on the [ComputationPool].
 */
suspend fun <A, B> Iterable<A>.parTraverseN(
  ctx: CoroutineContext = EmptyCoroutineContext,
  n: Long,
  f: suspend (A) -> B
): List<B> {
  val s = Semaphore(n)
  return parTraverse<A, B>(ctx) { a ->
    s.withPermit { f(a) }
  }
}

/**
 * Traverses this [Iterable] and runs all mappers [f] on [CoroutineContext].
 * Cancelling this operation cancels all running tasks.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.coroutines.*
 *
 * data class User(val id: Int)
 *
 * suspend fun main(): Unit {
 *   //sampleStart
 *   suspend fun getUserById(id: Int): User =
 *     User(id)
 *
 *   val res = listOf(1, 2, 3)
 *     .parTraverse(ComputationPool, ::getUserById)
 *  //sampleEnd
 *  println(res)
 * }
 * ```
 *
 * **WARNING**: operations run in parallel depending on the capabilities of the provided [CoroutineContext].
 * We ensure they start in sequence so it's guaranteed to finish on a single threaded context.
 *
 * @see parTraverse for a function that ensures it runs in parallel on the [ComputationPool].
 */ // Foldable impl, more performant implementations can be done for other data types
suspend fun <A, B> Iterable<A>.parTraverse(
  ctx: CoroutineContext = EmptyCoroutineContext,
  f: suspend (A) -> B
): List<B> = coroutineScope {
  map { async(ctx) { f.invoke(it) } }.awaitAll()
}

suspend fun <A, B, E> Iterable<A>.parTraverseEither(
  ctx: CoroutineContext = EmptyCoroutineContext,
  f: suspend (A) -> Either<E, B>
): Either<E, List<B>> =
  either {
    coroutineScope {
      map { async(ctx) { !f.invoke(it) } }.awaitAll()
    }
  }

suspend fun <A, B, E> Iterable<A>.parTraverseValidated(
  ctx: CoroutineContext = EmptyCoroutineContext,
  semigroup: Semigroup<E>,
  f: suspend (A) -> Validated<E, B>
): Validated<E, List<B>> =
  coroutineScope {
    map { async(ctx) { f.invoke(it) } }.awaitAll()
      .sequence(semigroup)
  }

// TODO should be in Arrow Core
inline fun <E, A, B> Iterable<A>.traverse(semigroup: Semigroup<E>, f: (A) -> Validated<E, B>): Validated<E, List<B>> =
  fold(emptyList<B>().valid() as Validated<E, List<B>>) { acc, a ->
    f(a).ap(semigroup, acc.map { bs: List<B> -> { b: B -> bs + b } })
  }

// TODO should be in Arrow Core
fun <E, A> Iterable<Validated<E, A>>.sequence(semigroup: Semigroup<E>): Validated<E, List<A>> =
  fold(emptyList<A>().valid() as Validated<E, List<A>>) { acc, a ->
    a.ap(semigroup, acc.map { bs: List<A> -> { b: A -> bs + b } })
  }
