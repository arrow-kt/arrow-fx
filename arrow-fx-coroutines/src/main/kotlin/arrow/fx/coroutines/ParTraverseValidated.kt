@file:JvmMultifileClass
@file:JvmName("ParTraverse")

package arrow.fx.coroutines

import arrow.core.Validated
import arrow.core.ap
import arrow.core.identity
import arrow.core.valid
import arrow.typeclasses.Semigroup
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

/**
 * Traverses this [Iterable] and runs [f] in [n] parallel operations on [CoroutineContext].
 * If one or more of the tasks returns [Validated.Invalid] then all the [Validated.Invalid] results will be combined using [semigroup].
 *
 * Cancelling this operation cancels all running tasks.
 */
suspend fun <E, A> Iterable<suspend () -> Validated<E, A>>.parSequenceValidatedN(semigroup: Semigroup<E>, n: Int): Validated<E, List<A>> =
  parTraverseValidatedN(Dispatchers.Default, semigroup, n) { it() }

/**
 * Traverses this [Iterable] and runs [f] in [n] parallel operations on [CoroutineContext].
 * If one or more of the tasks returns [Validated.Invalid] then all the [Validated.Invalid] results will be combined using [semigroup].
 *
 * Coroutine context is inherited from a [CoroutineScope], additional context elements can be specified with [ctx] argument.
 * If the combined context does not have any dispatcher nor any other [ContinuationInterceptor], then [Dispatchers.Default] is used.
 * **WARNING** If the combined context has a single threaded [ContinuationInterceptor], this function will not run in parallel.
 *
 * Cancelling this operation cancels all running tasks.
 */
suspend fun <E, A> Iterable<suspend () -> Validated<E, A>>.parSequenceValidatedN(
  ctx: CoroutineContext = EmptyCoroutineContext,
  semigroup: Semigroup<E>,
  n: Int
): Validated<E, List<A>> =
  parTraverseValidatedN(ctx, semigroup, n) { it() }

/**
 * Traverses this [Iterable] and runs [f] in [n] parallel operations on [Dispatchers.Default].
 * If one or more of the [f] returns [Validated.Invalid] then all the [Validated.Invalid] results will be combined using [semigroup].
 *
 * Cancelling this operation cancels all running tasks.
 */
suspend fun <E, A, B> Iterable<A>.parTraverseValidatedN(
  semigroup: Semigroup<E>,
  n: Int,
  f: suspend (A) -> Validated<E, B>
): Validated<E, List<B>> =
  parTraverseValidatedN(Dispatchers.Default, semigroup, n, f)

/**
 * Traverses this [Iterable] and runs [f] in [n] parallel operations on [CoroutineContext].
 * If one or more of the [f] returns [Validated.Invalid] then all the [Validated.Invalid] results will be combined using [semigroup].
 *
 * Coroutine context is inherited from a [CoroutineScope], additional context elements can be specified with [ctx] argument.
 * If the combined context does not have any dispatcher nor any other [ContinuationInterceptor], then [Dispatchers.Default] is used.
 * **WARNING** If the combined context has a single threaded [ContinuationInterceptor], this function will not run in parallel.
 *
 * Cancelling this operation cancels all running tasks.
 */
suspend fun <E, A, B> Iterable<A>.parTraverseValidatedN(
  ctx: CoroutineContext = EmptyCoroutineContext,
  semigroup: Semigroup<E>,
  n: Int,
  f: suspend (A) -> Validated<E, B>
): Validated<E, List<B>> {
  val semaphore = Semaphore(n)
  return parTraverseValidated(ctx, semigroup) { a ->
    semaphore.withPermit { f(a) }
  }
}

/**
 * Sequences all tasks in parallel on [Dispatchers.Default] and return the result.
 * If one or more of the tasks returns [Validated.Invalid] then all the [Validated.Invalid] results will be combined using [semigroup].
 *
 * Cancelling this operation cancels all running tasks.
 */
suspend fun <E, A> Iterable<suspend () -> Validated<E, A>>.parSequenceEither(semigroup: Semigroup<E>): Validated<E, List<A>> =
  parTraverseValidated(Dispatchers.Default, semigroup) { it() }

/**
 * Sequences all tasks in parallel on [ctx] and return the result.
 * If one or more of the tasks returns [Validated.Invalid] then all the [Validated.Invalid] results will be combined using [semigroup].
 *
 * Coroutine context is inherited from a [CoroutineScope], additional context elements can be specified with [ctx] argument.
 * If the combined context does not have any dispatcher nor any other [ContinuationInterceptor], then [Dispatchers.Default] is used.
 * **WARNING** If the combined context has a single threaded [ContinuationInterceptor], this function will not run in parallel.
 *
 * Cancelling this operation cancels all running tasks.
 *
 * ```kotlin:ank:playground
 * import arrow.core.*
 * import arrow.fx.coroutines.*
 * import arrow.core.extensions.nonemptylist.semigroup.semigroup
 * import kotlinx.coroutines.Dispatchers
 *
 * typealias Task = suspend () -> ValidatedNel<Throwable, Unit>
 *
 * suspend fun main(): Unit {
 *   //sampleStart
 *   fun getTask(id: Int): Task =
 *     suspend { Validated.catchNel { println("Working on task $id on ${Thread.currentThread().name}") } }
 *
 *   val res = listOf(1, 2, 3)
 *     .map(::getTask)
 *     .parSequenceValidated(Dispatchers.IO, NonEmptyList.semigroup())
 *   //sampleEnd
 *   println(res)
 * }
 * ```
 */
suspend fun <E, A> Iterable<suspend () -> Validated<E, A>>.parSequenceValidated(
  ctx: CoroutineContext = EmptyCoroutineContext,
  semigroup: Semigroup<E>
): Validated<E, List<A>> =
  parTraverseValidated(ctx, semigroup) { it() }

/**
 * Traverses this [Iterable] and runs all mappers [f] on [Dispatchers.Default].
 * If one or more of the [f] returns [Validated.Invalid] then all the [Validated.Invalid] results will be combined using [semigroup].
 *
 * Cancelling this operation cancels all running tasks.
 */
suspend fun <E, A, B> Iterable<A>.parTraverseValidated(
  semigroup: Semigroup<E>,
  f: suspend (A) -> Validated<E, B>
): Validated<E, List<B>> =
  parTraverseValidated(Dispatchers.Default, semigroup, f)

/**
 * Traverses this [Iterable] and runs all mappers [f] on [CoroutineContext].
 * If one or more of the [f] returns [Validated.Invalid] then all the [Validated.Invalid] results will be combined using [semigroup].
 *
 * Coroutine context is inherited from a [CoroutineScope], additional context elements can be specified with [ctx] argument.
 * If the combined context does not have any dispatcher nor any other [ContinuationInterceptor], then [Dispatchers.Default] is used.
 * **WARNING** If the combined context has a single threaded [ContinuationInterceptor], this function will not run in parallel.
 *
 * Cancelling this operation cancels all running tasks.
 *
 * ```kotlin:ank:playground
 * import arrow.core.*
 * import arrow.core.extensions.nonemptylist.semigroup.semigroup
 * import arrow.fx.coroutines.*
 * import kotlinx.coroutines.Dispatchers
 *
 * object Error
 * data class User(val id: Int, val createdOn: String)
 *
 * suspend fun main(): Unit {
 *   //sampleStart
 *   suspend fun getUserById(id: Int): ValidatedNel<Error, User> =
 *     if(id % 2 == 0) Error.invalidNel()
 *     else User(id, Thread.currentThread().name).validNel()
 *
 *   val res = listOf(1, 3, 5)
 *     .parTraverseValidated(Dispatchers.IO, NonEmptyList.semigroup(), ::getUserById)
 *
 *   val res2 = listOf(1, 2, 3, 4, 5)
 *     .parTraverseValidated(Dispatchers.IO, NonEmptyList.semigroup(), ::getUserById)
 *  //sampleEnd
 *  println(res)
 *  println(res2)
 * }
 * ```
 */
suspend fun <E, A, B> Iterable<A>.parTraverseValidated(
  ctx: CoroutineContext = EmptyCoroutineContext,
  semigroup: Semigroup<E>,
  f: suspend (A) -> Validated<E, B>
): Validated<E, List<B>> =
  coroutineScope {
    map { async(ctx) { f.invoke(it) } }.awaitAll()
      .sequence(semigroup)
  }

@PublishedApi // TODO should be in Arrow Core
internal inline fun <E, A, B> Iterable<A>.traverse(semigroup: Semigroup<E>, f: (A) -> Validated<E, B>): Validated<E, List<B>> =
  fold(emptyList<B>().valid() as Validated<E, List<B>>) { acc, a ->
    f(a).ap(semigroup, acc.map { bs: List<B> -> { b: B -> bs + b } })
  }

@PublishedApi // TODO should be in Arrow Core
internal fun <E, A> Iterable<Validated<E, A>>.sequence(semigroup: Semigroup<E>): Validated<E, List<A>> =
  traverse(semigroup, ::identity)
