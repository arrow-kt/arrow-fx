package arrow.fx

import arrow.Kind
import arrow.core.Either.Left
import arrow.core.EitherOf
import arrow.core.Either
import arrow.core.Eval
import arrow.core.NonFatal
import arrow.core.Option
import arrow.core.Right
import arrow.core.Some
import arrow.core.andThen
import arrow.core.identity
import arrow.core.nonFatalOrThrow
import arrow.core.fix
import arrow.fx.IO.Companion.async
import arrow.fx.IO.Companion.effect
import arrow.fx.IO.Pure
import arrow.fx.IO.RaiseError
import arrow.fx.OnCancel.Companion.CancellationException
import arrow.fx.OnCancel.Silent
import arrow.fx.OnCancel.ThrowCancellationException
import arrow.fx.extensions.io.async.effectMap
import arrow.fx.extensions.io.concurrent.concurrent
import arrow.fx.extensions.io.dispatchers.dispatchers
import arrow.fx.internal.ArrowInternalException
import arrow.fx.internal.ForwardCancellable
import arrow.fx.internal.IOBracket
import arrow.fx.internal.IOFiber
import arrow.fx.internal.IOForkedStart
import arrow.fx.internal.IOTick
import arrow.fx.internal.Platform.maxStackDepthSize
import arrow.fx.internal.Platform.onceOnly
import arrow.fx.internal.Platform.unsafeResync
import arrow.fx.internal.UnsafePromise
import arrow.fx.internal.scheduler
import arrow.fx.typeclasses.Disposable
import arrow.fx.typeclasses.Duration
import arrow.fx.typeclasses.ExitCase
import arrow.fx.typeclasses.ExitCase2
import arrow.fx.typeclasses.Fiber
import arrow.fx.typeclasses.mapUnit
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

sealed class IOResult<out E, out A> {
  data class Success<A>(val value: A) : IOResult<Nothing, A>()
  data class Error<E>(val error: E) : IOResult<E, Nothing>()
  data class Exception(val exception: Throwable) : IOResult<Nothing, Nothing>()

  fun <R> fold(ifException: (Throwable) -> R, ifLeft: (E) -> R, ifSuccess: (A) -> R): R =
    when (this) {
      is Success -> ifSuccess(this.value)
      is Error -> ifLeft(this.error)
      is Exception -> ifException(this.exception)
    }

  companion object
}

class ForIO private constructor() {
  companion object
}
typealias IOOf<E, A> = arrow.Kind2<ForIO, E, A>
typealias IOPartialOf<E> = Kind<ForIO, E>

inline fun <E, A> IOOf<E, A>.fix(): IO<E, A> =
  this as IO<E, A>

typealias IOProc<E, A> = ((IOResult<E, A>) -> Unit) -> Unit
typealias IOProcF<E, A> = ((IOResult<E, A>) -> Unit) -> IOOf<E, Unit>

@Suppress("StringLiteralDuplication")
sealed class IO<out E, out A> : IOOf<E, A> {

  companion object : IOParMap, IORace {

    /**
     * Delay a suspended effect.
     *
     * ```kotlin:ank:playground:extension
     * import arrow.fx.IO
     * import kotlinx.coroutines.Dispatchers
     * import arrow.fx.unsafeRunSync
     *
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   suspend fun helloWorld(): Unit = println("Hello World!")
     *
     *   val result = IO.effect { helloWorld() }
     *   //sampleEnd
     *   println(result.unsafeRunSync())
     * }
     * ```
     */
    fun <A> effect(f: suspend () -> A): IO<Nothing, A> =
      Effect(effect = f)

    /**
     * Delay a suspended effect which results in an Either.
     *
     * @return a success [IO] when [f] returns [Either.Right]<[A]> or an error when [f] returns [Either.Left]<[E]>
     *
     * ```kotlin:ank:playground:extension
     * import arrow.fx.IO
     * import kotlinx.coroutines.Dispatchers
     * import arrow.fx.unsafeRunSync
     *
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   suspend fun helloWorld(): Either<Nothing, Unit> = Either.catch { println("Hello World!") }
     *
     *   val result = IO.effectEither { helloWorld() }
     *   //sampleEnd
     *   result.unsafeRunSync()
     * }
     * ```
     *
     */
    fun <E, A> effectEither(f: suspend () -> EitherOf<E, A>): IO<E, A> =
      Effect(effect = f).flattenEither()

    /**
     * Delay a suspended effect which results in an Either on provided [CoroutineContext].
     *
     * @return a success [IO] when [f] returns [Either.Right]<[A]> or an error when [f] returns [Either.Left]<[E]>
     *
     * ```kotlin:ank:playground:extension
     * import arrow.fx.IO
     * import kotlinx.coroutines.Dispatchers
     *
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   suspend fun getThreadSuspended() = Either.right(Thread.currentThread().name)
     *
     *   val result = IO.effect(Dispatchers.Default) { getThreadSuspended() }
     *   //sampleEnd
     *   println(result)
     * }
     * ```
     */
    fun <E, A> effectEither(ctx: CoroutineContext, f: suspend () -> EitherOf<E, A>): IO<E, A> =
      Effect(ctx, effect = f).flattenEither()

    /**
     * Delay a suspended effect on provided [CoroutineContext].
     *
     * @param ctx [CoroutineContext] to run evaluation on.
     *
     * ```kotlin:ank:playground:extension
     * import arrow.fx.IO
     * import kotlinx.coroutines.Dispatchers
     *
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   suspend fun getThreadSuspended(): String = Thread.currentThread().name
     *
     *   val result = IO.effect(Dispatchers.Default) { getThreadSuspended() }
     *   //sampleEnd
     *   println(result)
     * }
     * ```
     */
    fun <A> effect(ctx: CoroutineContext, f: suspend () -> A): IO<Nothing, A> =
      Effect(ctx, f)

    /** @see effect */
    operator fun <A> invoke(ctx: CoroutineContext, f: suspend () -> A): IO<Nothing, A> =
      effect(ctx, f)

    /** @see effect */
    operator fun <A> invoke(f: suspend () -> A): IO<Nothing, A> =
      effect(EmptyCoroutineContext, f)

    /**
     * Just wrap a pure value [A] into [IO].
     *
     * ```kotlin:ank:playground
     * import arrow.fx.IO
     * import arrow.fx.unsafeRunSync
     *
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   val result = IO.just("Hello from just!")
     *   //sampleEnd
     *   println(result.unsafeRunSync())
     * }
     * ```
     */
    fun <A> just(a: A): IO<Nothing, A> = Pure(a)

    /**
     * Raise an exception in a pure way without actually throwing.
     *
     * ```kotlin:ank:playground
     * import arrow.fx.IO
     * import arrow.fx.unsafeRunSync
     *
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   val result: IO<Nothing, Int> = IO.raiseException<Int>(RuntimeException("Boom"))
     *   //sampleEnd
     *   println(result.unsafeRunSync())
     * }
     * ```
     */
    fun <A> raiseException(e: Throwable): IO<Nothing, A> = RaiseException(e)

    /**
     * Raise an error in a pure way
     *
     * ```kotlin:ank:playground
     * import arrow.fx.IO
     *
     * object NetworkError
     *
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   val result: IO<NetworkError, Int> = IO.raiseError(NetworkError)
     *   //sampleEnd
     *   println(result.unsafeRunSyncEither())
     * }
     * ```
     */
    fun <E, A> raiseError(e: E): IO<E, A> = RaiseError(e)

    /**
     *  Sleeps for a given [duration] without blocking a thread.
     *
     * ```kotlin:ank:playground
     * import arrow.fx.*
     * import arrow.fx.typeclasses.seconds
     *
     * fun main(args: Array<String>) {
     *   val result =
     *     //sampleStart
     *     IO.sleep(3.seconds).flatMap {
     *       IO.effect { println("Hello World!") }
     *     }
     *   //sampleEnd
     *   result.unsafeRunSync()
     * }
     * ```
     **/
    fun sleep(duration: Duration, continueOn: CoroutineContext = IODispatchers.CommonPool): IO<Nothing, Unit> =
      cancellable { cb ->
        val cancelRef = scheduler.schedule(IOTick(continueOn, cb), duration.amount, duration.timeUnit)
        later { cancelRef.cancel(false); Unit }
      }

    /**
     * Wraps a function into [IO] to execute it _later_.
     *
     * @param f function to wrap into [IO].
     *
     * ```kotlin:ank:playground
     * import arrow.fx.IO
     * import arrow.fx.unsafeRunSync
     *
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   val result = IO { "Hello from operator invoke!" }
     *   //sampleEnd
     *   println(result.unsafeRunSync())
     * }
     * ```
     */
    fun <A> later(f: () -> A): IO<Nothing, A> =
      defer { Pure(f()) }

    /**
     * Defer a computation that results in an [IO] value.
     *
     * ```kotlin:ank:playground
     * import arrow.fx.IO
     * import arrow.fx.unsafeRunSync
     *
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   val result = IO.defer { IO { "Hello from IO in defer" } }
     *   //sampleEnd
     *   println(result.unsafeRunSync())
     * }
     * ```
     */
    fun <E, A> defer(f: () -> IOOf<E, A>): IO<E, A> =
      Suspend(f)

    /**
     * Create an [IO] that executes an asynchronous process on evaluation.
     * This combinator can be used to wrap callbacks or other similar impure code **that require no cancellation code**.
     *
     * ```kotlin:ank:playground
     * import arrow.fx.*
     * import java.lang.RuntimeException
     *
     * typealias Callback = (List<String>?, Throwable?) -> Unit
     * class GithubId
     * object GithubService {
     *   fun getUsernames(callback: Callback) {
     *     //execute operation and call callback at some point in future
     *   }
     * }
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   fun getUsernames(): IO<Nothing, List<String>> =
     *     IO.async { cb: (IOResult<Nothing, List<String>>) -> Unit ->
     *       GithubService.getUsernames { names, throwable ->
     *         when {
     *           names != null -> cb(IOResult.Success(names))
     *           throwable != null -> cb(IOResult.Exception(throwable))
     *           else -> cb(IOResult.Exception(RuntimeException("Null result and no exception")))
     *         }
     *       }
     *     }
     *   val result = getUsernames()
     *   //sampleEnd
     *   println(result.unsafeRunSync())
     * }
     * ```
     *
     * @param k an asynchronous computation that might fail typed as [IOProc].
     * @see cancellable for an operator that supports cancellation.
     * @see asyncF for a version that can suspend side effects in the registration function.
     */
    fun <E, A> async(k: IOProc<E, A>): IO<E, A> =
      Async { _: IOConnection, ff: (IOResult<E, A>) -> Unit ->
        onceOnly(ff).let { callback: (IOResult<E, A>) -> Unit ->
          try {
            k(callback)
          } catch (throwable: Throwable) {
            callback(IOResult.Exception(throwable.nonFatalOrThrow()))
          }
        }
      }

    /**
     * Create an [IO] that executes an asynchronous process on evaluation.
     * This combinator can be used to wrap callbacks or other similar impure code **that require no cancellation code**.
     *
     * ```kotlin:ank:playground
     * import arrow.fx.*
     * import java.lang.RuntimeException
     *
     * typealias Callback = (List<String>?, Throwable?) -> Unit
     *
     * object GithubService {
     *   fun getUsernames(callback: Callback) {
     *     //execute operation and call callback at some point in future
     *   }
     * }
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   fun getUsernames(): IO<Nothing, List<String>> =
     *     IO.asyncF { cb: (IOResult<Nothing, List<String>>) -> Unit ->
     *       IO {
     *         GithubService.getUsernames { names, throwable ->
     *           when {
     *             names != null -> cb(IOResult.Success(names))
     *             throwable != null -> cb(IOResult.Exception(throwable))
     *             else -> cb(IOResult.Exception(RuntimeException("Null result and no exception")))
     *           }
     *         }
     *       }
     *     }
     *   val result = getUsernames()
     *   //sampleEnd
     *   println(result.unsafeRunSync())
     * }
     * ```
     *
     * @param k a deferred asynchronous computation that might fail typed as [IOProcF].
     * @see async for a version that can suspend side effects in the registration function.
     * @see cancellableF for an operator that supports cancellation.
     */
    fun <E, A> asyncF(k: IOProcF<E, A>): IO<E, A> =
      Async { conn: IOConnection, ff: (IOResult<E, A>) -> Unit ->
        val conn2 = IOConnection()
        conn.push(conn2.cancel())
        onceOnly(conn, ff).let { callback: (IOResult<E, A>) -> Unit ->
          val fa = try {
            k(callback)
          } catch (t: Throwable) {
            if (NonFatal(t)) {
              IO { callback(IOResult.Exception(t)) }
            } else {
              throw t
            }
          }

          IORunLoop.startCancellable(fa, conn2) { result ->
            result.fold({ e -> callback(IOResult.Exception(e)) }, { e -> callback(IOResult.Error(e)) }, mapUnit)
          }
        }
      }

    /**
     * Creates a cancellable instance of [IO] that executes an asynchronous process on evaluation.
     * This combinator can be used to wrap callbacks or other similar impure code that requires cancellation code.
     *
     * ```kotlin:ank:playground
     * import arrow.fx.*
     * import java.lang.RuntimeException
     * import arrow.fx.unsafeRunSync
     *
     * typealias Callback = (List<String>?, Throwable?) -> Unit
     * class GithubId
     *
     * object GithubService {
     *   private val listeners: MutableMap<GithubId, Callback> = mutableMapOf()
     *   fun getUsernames(callback: Callback): GithubId {
     *     val id = GithubId()
     *     listeners[id] = callback
     *     //execute operation and call callback at some point in future
     *     return id
     *   }
     *   fun unregisterCallback(id: GithubId): Unit {
     *     listeners.remove(id)
     *   }
     * }
     *
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   fun getUsernames(): IO<Nothing, List<String>> =
     *     IO.cancellable { cb: (IOResult<Nothing, List<String>>) -> Unit ->
     *       val id = GithubService.getUsernames { names, throwable ->
     *         when {
     *           names != null -> cb(IOResult.Success(names))
     *           throwable != null -> cb(IOResult.Exception(throwable))
     *           else -> cb(IOResult.Exception(RuntimeException("Null result and no exception")))
     *         }
     *       }
     *       IO { GithubService.unregisterCallback(id) }
     *     }
     *   val result = getUsernames()
     *   //sampleEnd
     *   println(result.unsafeRunSync())
     * }
     * ```
     *
     * @param cb an asynchronous computation that might fail.
     * @see async for wrapping impure APIs without cancellation
     */
    fun <E, A> cancellable(cb: ((IOResult<E, A>) -> Unit) -> IOOf<E, Unit>): IO<E, A> =
      Async { conn: IOConnection, cbb: (IOResult<E, A>) -> Unit ->
        onceOnly(conn, cbb).let { cbb2 ->
          val cancellable = ForwardCancellable()
          conn.push(cancellable.cancel())
          if (conn.isNotCancelled()) {
            cancellable.complete(try {
              cb(cbb2)
            } catch (throwable: Throwable) {
              cbb2(IOResult.Exception(throwable.nonFatalOrThrow()))
              unit
            })
          }
        }
      }

    @Deprecated("Renaming this api for consistency", ReplaceWith("cancellable(cb)"))
    fun <E, A> cancelable(cb: ((IOResult<E, A>) -> Unit) -> IOOf<E, Unit>): IO<E, A> =
      cancellable(cb)

    /**
     * Creates a cancellable instance of [IO] that executes an asynchronous process on evaluation.
     * This combinator can be used to wrap callbacks or other similar impure code that requires cancellation code.
     *
     * ```kotlin:ank:playground
     * import arrow.fx.*
     * import java.lang.RuntimeException
     *
     * typealias Callback = (List<String>?, Throwable?) -> Unit
     * class GithubId
     *
     * object GithubService {
     *   private val listeners: MutableMap<GithubId, Callback> = mutableMapOf()
     *   fun getUsernames(callback: Callback): GithubId {
     *     val id = GithubId()
     *     listeners[id] = callback
     *     //execute operation and call callback at some point in future
     *     return id
     *   }
     *   fun unregisterCallback(id: GithubId): Unit {
     *     listeners.remove(id)
     *   }
     * }
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   fun getUsernames(): IO<Nothing, List<String>> =
     *     IO.cancellableF { cb: (IOResult<Nothing, List<String>>) -> Unit ->
     *       IO {
     *         val id = GithubService.getUsernames { names, throwable ->
     *           when {
     *             names != null -> cb(IOResult.Success(names))
     *             throwable != null -> cb(IOResult.Exception(throwable))
     *             else -> cb(IOResult.Exception(RuntimeException("Null result and no exception")))
     *           }
     *         }
     *         IO { GithubService.unregisterCallback(id) }
     *       }
     *     }
     *   val result = getUsernames()
     *   //sampleEnd
     *   println(result.unsafeRunSync())
     * }
     * ```
     *
     * @param cb a deferred asynchronous computation that might fail.
     * @see asyncF for wrapping impure APIs without cancellation
     */
    fun <E, A> cancellableF(cb: ((IOResult<E, A>) -> Unit) -> IOOf<E, IOOf<E, Unit>>): IO<E, A> =
      Async { conn: IOConnection, cbb: (IOResult<E, A>) -> Unit ->
        val cancellable = ForwardCancellable()
        val conn2 = IOConnection()
        conn.push(cancellable.cancel())
        conn.push(conn2.cancel())

        onceOnly(conn, cbb).let { cbb2 ->
          val fa: IOOf<E, IOOf<E, Unit>> = try {
            cb(cbb2)
          } catch (throwable: Throwable) {
            cbb2(IOResult.Exception(throwable.nonFatalOrThrow()))
            just(unit)
          }

          IORunLoop.startCancellable(fa, conn2) { result ->
            conn.pop()
            result.fold({}, {}, { cancellable.complete(it) })
          }
        }
      }

    @Deprecated("Renaming this api for consistency", ReplaceWith("cancellableF(cb)"))
    fun <E, A> cancelableF(cb: ((IOResult<E, A>) -> Unit) -> IOOf<E, IOOf<E, Unit>>): IO<E, A> =
      cancellableF(cb)

    /**
     * A pure [IO] value of [Unit].
     *
     * ```kotlin:ank:playground
     * import arrow.fx.IO
     * import arrow.fx.unsafeRunSync
     *
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   val result = IO.unit
     *   //sampleEnd
     *   println(result.unsafeRunSync())
     * }
     * ```
     */
    val unit: IO<Nothing, Unit> =
      just(Unit)

    /**
     * A lazy [IO] value of [Unit].
     *
     * ```kotlin:ank:playground
     * import arrow.fx.IO
     * import arrow.fx.unsafeRunSync
     *
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   val result = IO.lazy
     *   //sampleEnd
     *   println(result.unsafeRunSync())
     * }
     * ```
     */
    val lazy: IO<Nothing, Unit> =
      invoke { }

    /**
     * Evaluates an [Eval] instance within a safe [IO] context.
     *
     * ```kotlin:ank:playground
     * import arrow.fx.IO
     * import arrow.core.Eval
     * import arrow.fx.unsafeRunSync
     *
     * fun main(args: Array<String>) {
     *   fun longCalculation(): Int = 9999
     *   //sampleStart
     *   val result = IO.eval(Eval.later { longCalculation() })
     *   //sampleEnd
     *   println(result.unsafeRunSync())
     * }
     * ```
     */
    fun <A> eval(eval: Eval<A>): IO<Nothing, A> =
      when (eval) {
        is Eval.Now -> just(eval.value)
        else -> invoke { eval.value() }
      }

    /**
     * Perform a recursive operation in a stack-safe way, by checking the inner [Either] value.
     * If you want to continue the recursive operation return [Either.Left] with the intermediate result [A],
     * [Either.Right] indicates the terminal event and *must* thus return the resulting value [B].
     *
     * ```kotlin:ank:playground
     * import arrow.core.*
     * import arrow.fx.IO
     * import arrow.fx.unsafeRunSync
     *
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   val result = IO.tailRecM(0) { i ->
     *     IO.just(
     *      if(i == 5000) Right(i)
     *      else Left(i + 1)
     *     )
     *   }
     *   //sampleEnd
     *   println(result.unsafeRunSync())
     * }
     * ```
     */
    fun <E, A, B> tailRecM(a: A, f: (A) -> IOOf<E, Either<A, B>>): IO<E, B> =
      f(a).fix().flatMap {
        when (it) {
          is Left -> tailRecM(it.a, f)
          is Either.Right -> just(it.b)
        }
      }

    /**
     * A pure [IO] value that never returns.
     * Useful when you need to model non-terminating cases.
     *
     * ```kotlin:ank:playground
     * import arrow.fx.IO
     * import arrow.fx.unsafeRunSync
     *
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   val result: IO<Nothing, Int> = IO.never
     *   //sampleEnd
     *   println(result.unsafeRunSync())
     * }
     * ```
     */
    val never: IO<Nothing, Nothing> = async { }
  }

  /**
   * Run the [IO] in a suspended environment.
   *
   * ```kotlin:ank:playground
   * import arrow.core.Either
   * import arrow.fx.IO
   *
   * //sampleStart
   * suspend fun main(args: Array<String>): Either<Nothing, Unit> =
   *   IO.effect { println("Hello World!") }
   *   .suspended()
   * //sampleEnd
   * ```
   */
  suspend fun suspended(): Either<E, A> = suspendCoroutine { cont ->
    val connection = cont.context[IOContext]?.connection ?: IOConnection.uncancellable

    IORunLoop.startCancellable(this, connection) {
      it.fold(cont::resumeWithException, { e -> cont.resume(Left(e)) }, { a -> cont.resume(Right(a)) })
    }
  }

  /**
   * Transform the [IO] wrapped value of [A] into [B] preserving the [IO] structure.
   *
   * @param f a pure function that maps the value [A] to a value [B].
   * @returns an [IO] that results in a value [B].
   *
   * ```kotlin:ank:playground
   * import arrow.fx.IO
   * import arrow.fx.unsafeRunSync
   *
   * fun main(args: Array<String>) {
   *   val result =
   *   //sampleStart
   *   IO.just("Hello").map { "$it World" }
   *   //sampleEnd
   *   println(result.unsafeRunSync())
   * }
   * ```
   */
  open fun <B> map(f: (A) -> B): IO<E, B> =
    Map(this, f, 0)

  /**
   * Continue the evaluation on provided [CoroutineContext]
   *
   * @param ctx [CoroutineContext] to run evaluation on
   * @returns an [IO] that'll run the following computations on [ctx].
   *
   * ```kotlin:ank:playground
   * import arrow.fx.IO
   * import kotlinx.coroutines.Dispatchers
   * import arrow.fx.unsafeRunSync
   *
   * fun main(args: Array<String>) {
   *   //sampleStart
   *   val result = IO.unit.continueOn(Dispatchers.Default).flatMap {
   *     IO { Thread.currentThread().name }
   *   }
   *   //sampleEnd
   *   println(result.unsafeRunSync())
   * }
   * ```
   */
  open fun continueOn(ctx: CoroutineContext): IO<E, A> =
    ContinueOn(this, ctx)

  /**
   * Safely attempts the [IO] and lift any errors to the value side into [Either].
   *
   * ```kotlin:ank:playground
   * import arrow.fx.IO
   * import arrow.fx.unsafeRunSync
   *
   * fun main(args: Array<String>) {
   *   //sampleStart
   *   val resultA = IO.raiseException<Int>(RuntimeException("Boom!")).attempt()
   *   val resultB = IO.just("Hello").attempt()
   *   //sampleEnd
   *   println("resultA: ${resultA.unsafeRunSync()}, resultB: ${resultB.unsafeRunSync()}")
   * }
   * ```
   *
   * @see flatMap if you need to act on the output of the original [IO].
   */
  fun attempt(): IO<E, Either<Throwable, A>> =
    Bind(this, IOFrame.attempt())

  fun result(): IO<Nothing, IOResult<E, A>> =
    Bind(this, IOFrame.result())

  /**
   * [runAsync] allows you to run any [IO] in a referential transparent manner.
   *
   * Reason it can happen in a referential transparent manner is because nothing is actually running when this method is invoked.
   * The combinator can be used to define how several programs have to run in a safe manner.
   */
  fun runAsync(cb: (IOResult<E, A>) -> IOOf<Nothing, Unit>): IO<Nothing, Unit> =
    IO { unsafeRunAsyncEither(cb.andThen { it.fix().unsafeRunAsync { } }) }

  /**
   * [unsafeRunAsync] allows you to run any [IO] and receive the values in a callback [cb]
   * and thus **has** the ability to run `NonBlocking` but that depends on the implementation.
   * When the underlying effects/program runs blocking on the callers thread this method will run blocking.
   *
   * To start this on `NonBlocking` use `NonBlocking.shift().followedBy(io).unsafeRunAsync { }`.
   *
   * @param cb the callback that is called with the computations result represented as an [Either].
   * @see [unsafeRunAsyncCancellable] to run in a cancellable manner.
   * @see [runAsync] to run in a referential transparent manner.
   */
  fun unsafeRunAsyncEither(cb: (IOResult<E, A>) -> Unit): Unit =
    IORunLoop.start(this, cb)

  /**
   * A pure version of [unsafeRunAsyncCancellable], it defines how an [IO] is ran in a cancellable manner but it doesn't run yet.
   *
   * It receives the values in a callback [cb] and thus **has** the ability to run `NonBlocking` but that depends on the implementation.
   * When the underlying effects/program runs blocking on the callers thread this method will run blocking.
   *
   * @param cb the callback that is called with the computations result represented as an [Either].
   * @return a [Disposable] that can be used to cancel the computation.
   * @see [unsafeRunAsync] to run in an unsafe and non-cancellable manner.
   * @see [unsafeRunAsyncCancellable] to run in a non-referential transparent manner.
   */
  fun runAsyncCancellable(onCancel: OnCancel = Silent, cb: (IOResult<E, A>) -> IOOf<Nothing, Unit>): IO<Nothing, Disposable> =
    Async { _, ccb ->
      val conn = IOConnection()
      val onCancelCb = when (onCancel) {
        ThrowCancellationException -> cb andThen { it.fix().unsafeRunAsync { } }
        Silent -> { either -> either.fold({ if (!conn.isCancelled() || it != CancellationException) cb(either) }, { cb(either); Unit }, { cb(either); Unit }) }
      }
      ccb(IOResult.Success(conn.toDisposable()))
      IORunLoop.startCancellable(this, conn, onCancelCb)
    }

  /**
   * [unsafeRunAsyncCancellable] allows you to run any [IO] and receive the values in a callback [cb] while being cancellable.
   * It **has** the ability to run `NonBlocking` but that depends on the implementation, when the underlying
   * effects/program runs blocking on the callers thread this method will run blocking.
   *
   * To start this on `NonBlocking` use `NonBlocking.shift().followedBy(io).unsafeRunAsync { }`.
   *
   * @param cb the callback that is called with the computations result represented as an [Either].
   * @returns [Disposable] or cancel reference that cancels the running [IO].
   * @see [unsafeRunAsyncCancellable] to run in a cancellable manner.
   * @see [runAsync] to run in a referential transparent manner.
   */
  fun unsafeRunAsyncCancellableEither(onCancel: OnCancel = Silent, cb: (IOResult<E, A>) -> Unit): Disposable =
    runAsyncCancellable(onCancel, cb andThen { unit }).unsafeRunSync()

  /**
   * [unsafeRunSync] allows you to run any [IO] to its wrapped value [A].
   *
   * It's called unsafe because it immediately runs the effects wrapped in [IO],
   * and thus is **not** referentially transparent.
   *
   * **NOTE** this function is intended for testing, it should never appear in your mainline production code!
   *
   * @return the resulting value
   * @see [unsafeRunAsync] or [unsafeRunAsyncCancellable] that run the value as [Either].
   * @see [runAsync] to run in a referential transparent manner.
   */
  fun unsafeRunSyncEither(): Either<E, A> =
    unsafeRunTimed(Duration.INFINITE)
      .fold({ throw IOTimedResultedException }, ::identity)

  /**
   * Run with a limitation on how long to await for *individual* async results.
   * It's possible that this methods runs forever i.e. for an infinite recursive [IO].
   *
   * **NOTE** this function is intended for testing, it should never appear in your mainline production code!
   *
   * @see unsafeRunSync
   */
  fun unsafeRunTimed(limit: Duration): Option<Either<E, A>> =
    IORunLoop.step(this).unsafeRunTimedTotal(limit)

  internal abstract fun unsafeRunTimedTotal(limit: Duration): Option<Either<E, A>>

  /** Makes the source [IO] uncancellable such that a [Fiber.cancel] signal has no effect. */
  fun uncancellable(): IO<E, A> =
    ContextSwitch(this, ContextSwitch.makeUncancellable, ContextSwitch.disableUncancellable)

  internal class Pure<out A>(val a: A) : IO<Nothing, A>() {
    // Pure can be replaced by its value
    override fun <B> map(f: (A) -> B): IO<Nothing, B> = Suspend { Pure(f(a)) }

    override fun unsafeRunTimedTotal(limit: Duration): Option<Either<Nothing, A>> = Some(Right(a))
  }

  internal class RaiseException(val exception: Throwable) : IO<Nothing, Nothing>() {
    // Errors short-circuit
    override fun <B> map(f: (Nothing) -> B): IO<Nothing, B> = this

    override fun unsafeRunTimedTotal(limit: Duration): Option<Nothing> = throw exception
  }

  internal class RaiseError<E>(val error: E) : IO<E, Nothing>() {
    // Errors short-circuit
    override fun <B> map(f: (Nothing) -> B): IO<E, B> = this

    override fun unsafeRunTimedTotal(limit: Duration): Option<Either<E, Nothing>> = Some(Left(error))
  }

  internal class Delay<out A>(val thunk: () -> A) : IO<Nothing, A>() {
    override fun unsafeRunTimedTotal(limit: Duration): Option<Nothing> = throw AssertionError("Unreachable")
  }

  internal class Suspend<out E, out A>(val thunk: () -> IOOf<E, A>) : IO<E, A>() {
    override fun unsafeRunTimedTotal(limit: Duration): Option<Nothing> = throw AssertionError("Unreachable")
  }

  internal class Async<out E, out A>(val shouldTrampoline: Boolean = false, val k: (IOConnection, (IOResult<E, A>) -> Unit) -> Unit) : IO<E, A>() {
    override fun unsafeRunTimedTotal(limit: Duration): Option<Either<E, A>> = unsafeResync(this, limit)
  }

  internal class Effect<out A>(val ctx: CoroutineContext? = null, val effect: suspend () -> A) : IO<Nothing, A>() {
    override fun unsafeRunTimedTotal(limit: Duration): Option<Either<Nothing, A>> = unsafeResync(this, limit)
  }

  internal class Bind<A, E, out B, out E2 : E>(val cont: IOOf<E, A>, val g: (A) -> IOOf<E, B>) : IO<E2, B>() {
    override fun unsafeRunTimedTotal(limit: Duration): Option<Nothing> = throw AssertionError("Unreachable")
  }

  internal class ContinueOn<E, A>(val cont: IO<E, A>, val cc: CoroutineContext) : IO<E, A>() {
    // If a ContinueOn follows another ContinueOn, execute only the latest
    override fun continueOn(ctx: CoroutineContext): IO<E, A> = ContinueOn(cont, ctx)

    override fun unsafeRunTimedTotal(limit: Duration): Option<Nothing> = throw AssertionError("Unreachable")
  }

  internal class ContextSwitch<E, A>(
    val source: IO<E, A>,
    val modify: (IOConnection) -> IOConnection,
    val restore: ((a: Any?, e: Any?, t: Throwable?, old: IOConnection, new: IOConnection) -> IOConnection)?
  ) : IO<E, A>() {
    override fun unsafeRunTimedTotal(limit: Duration): Option<Nothing> = throw AssertionError("Unreachable")

    companion object {
      // Internal reusable reference.
      internal val makeUncancellable: (IOConnection) -> IOConnection = { IOConnection.uncancellable }

      internal val disableUncancellable: (Any?, Any?, Throwable?, IOConnection, IOConnection) -> IOConnection =
        { _, _, _, old, _ -> old }
    }
  }

  internal class Map<A, out E, out B>(val source: IOOf<E, A>, val g: (A) -> B, val index: Int) : IO<E, B>(), (A) -> IO<E, B> {
    override fun invoke(value: A): IO<E, B> = just(g(value))

    override fun <C> map(f: (B) -> C): IO<E, C> =
    // Allowed to do maxStackDepthSize map operations in sequence before
      // starting a new Map fusion in order to avoid stack overflows
      if (index != maxStackDepthSize) Map(source, g.andThen(f), index + 1)
      else Map(this, f, 0)

    override fun unsafeRunTimedTotal(limit: Duration): Option<Nothing> = throw AssertionError("Unreachable")
  }
}

/**
 * Handle the error by mapping the error to a value of [A].
 *
 * ```kotlin:ank:playground
 * import arrow.fx.*
 *
 * fun main(args: Array<String>) {
 *   //sampleStart
 *   val result = IO.raiseException<Int>(RuntimeException("Boom"))
 *     .handleError { e -> "Goodbye World! after $e" }
 *   //sampleEnd
 *   println(result.unsafeRunSync())
 * }
 * ```
 *
 * @see handleErrorWith for a version that can resolve the error using an effect
 */
fun <E, A> IOOf<E, A>.handleError(f: (Throwable) -> A, fe: (E) -> A): IO<Nothing, A> =
  handleErrorWith({ t -> IO.Pure(f(t)) }, { e -> IO.Pure(fe(e)) })

fun <A> IOOf<Nothing, A>.handleError(f: (Throwable) -> A): IO<Nothing, A> =
  handleError(f, ::identity)

/**
 * Handle the error by resolving the error with an effect that results in [A].
 *
 * ```kotlin:ank:playground
 * import arrow.fx.*
 * import arrow.fx.typeclasses.milliseconds
 *
 * fun main(args: Array<String>) {
 *   fun getMessage(e: Throwable): IO<Nothing, String> = IO.sleep(250.milliseconds)
 *     .followedBy(IO.effect { "Delayed goodbye World! after $e" })
 *   //sampleStart
 *   val result = IO.raiseException<Int>(RuntimeException("Boom"))
 *     .handleErrorWith { e -> getMessage(e) }
 *   //sampleEnd
 *   println(result.unsafeRunSync())
 * }
 * ```
 */
fun <E, A, E2 : E> IOOf<E, A>.handleErrorWith(f: (Throwable) -> IOOf<E2, A>, fe: (E) -> IOOf<E2, A>): IO<E2, A> =
  IO.Bind(this, IOFrame.Companion.ErrorHandler(f, fe))

fun <A> IOOf<Nothing, A>.handleErrorWith(f: (Throwable) -> IOOf<Nothing, A>): IO<Nothing, A> =
  handleErrorWith(f, ::identity)

fun <E, A> IOOf<E, A>.fallbackTo(f: () -> A): IOOf<E, A> =
  handleError({ f() }, { f() })

fun <E, A, E2> IOOf<E, A>.fallbackWith(fa: IOOf<E2, A>): IO<E2, A> =
  handleErrorWith({ fa }, { fa })

/**
 * Redeem an [IO] to an [IO] of [B] by resolving the error **or** mapping the value [A] to [B].
 *
 * ```kotlin:ank:playground
 * import arrow.core.identity
 * import arrow.fx.*
 *
 * fun main(args: Array<String>) {
 *   val result =
 *     //sampleStart
 *     IO.raiseException<Int>(RuntimeException("Hello from Error"))
 *       .redeem({ e -> e.message ?: "" }, ::identity, Int::toString)
 *   //sampleEnd
 *   println(result.unsafeRunSync())
 * }
 * ```
 */
fun <E, A, B> IOOf<E, A>.redeem(ft: (Throwable) -> B, fe: (E) -> B, fb: (A) -> B): IO<Nothing, B> =
  IO.Bind(this, IOFrame.Companion.Redeem(ft, fe, fb))

/**
 * Redeem an [IO] to an [IO] of [B] by resolving the error **or** mapping the value [A] to [B] **with** an effect.
 *
 * ```kotlin:ank:playground
 * import arrow.core.identity
 * import arrow.fx.*
 *
 * fun main(args: Array<String>) {
 *   val result =
 *     //sampleStart
 *     IO.just("1")
 *       .redeemWith({ e -> IO.just(-1) }, ::identity, { str -> IO { str.toInt() } })
 *   //sampleEnd
 *   println(result.unsafeRunSync())
 * }
 * ```
 */
fun <E, A, E2 : E, B> IOOf<E, A>.redeemWith(
  ft: (Throwable) -> IOOf<E2, B>,
  fe: (E) -> IOOf<E2, B>,
  fb: (A) -> IOOf<E2, B>
): IO<E2, B> =
  IO.Bind(this, IOFrame.Companion.RedeemWith(ft, fe, fb))

/**
 * Executes the given `finalizer` when the source is finished, either in success or in error, or if cancelled, allowing
 * for differentiating between exit conditions. That's thanks to the [ExitCase] argument of the finalizer.
 *
 * As best practice, it's not a good idea to release resources via `guaranteeCase` in polymorphic code.
 * Prefer [bracketCase] for the acquisition and release of resources.
 *
 * @see [guarantee] for the simpler version
 * @see [bracketCase] for the more general operation
 *
 */
fun <E, A> IOOf<E, A>.guaranteeCase(finalizer: (ExitCase2<E>) -> IOOf<E, Unit>): IO<E, A> =
  IOBracket.guaranteeCase(this, finalizer)

/**
 * Transform the [IO] value of [A] by sequencing an effect [IO] that results in [B].
 *
 * @param f function that returns the [IO] effect resulting in [B] based on the input [A].
 * @returns an effect that results in [B].
 *
 * ```kotlin:ank:playground
 * import arrow.fx.IO
 * import arrow.fx.unsafeRunSync
 *
 * fun main(args: Array<String>) {
 *   val result =
 *   //sampleStart
 *   IO.just("Hello").flatMap { IO { "$it World" } }
 *   //sampleEnd
 *   println(result.unsafeRunSync())
 * }
 * ```
 */
fun <E, A, B, E2 : E> IOOf<E, A>.flatMap(f: (A) -> IOOf<E2, B>): IO<E2, B> =
  when (val current = fix()) {
    is IO.RaiseException -> current
    is IO.RaiseError<E> -> current as IO<E2, B>
    is IO.Pure<A> -> IO.Suspend { f(current.a) }
    else -> IO.Bind(current, f)
  }

/**
 * Flatten an [IO] with a success of [Either] into an [IO] of [A], when the left is the same error ([E]) as the original [IO] and the right is [A].
 *
 * @returns success if [Either.Right] or raises [E] otherwise.
 *
 *  * ```kotlin:ank:playground
 * import arrow.fx.IO
 * import arrow.fx.unsafeRunSync
 * import arrow.core.Either
 *
 * fun main(args: Array<String>) {
 *   val result =
 *   //sampleStart
 *   IO.just(Either.right("Hello")).flattenEither().flatMap { IO { "$it World" } }
 *   //sampleEnd
 *   println(result.unsafeRunSync())
 * }
 * ```
 */
fun <E, A> IO<E, EitherOf<E, A>>.flattenEither(): IO<E, A> =
  flatMap { it.fix().fold(::RaiseError, ::Pure) }

/**
 * Transform the value of an [IO] into an [Either] and consequently flatten into an [IO]
 *
 * @return success when [f] results in [Either.Right] or error when [f] results in [Either.Left]
 *
 * fun main(args: Array<String>) {
 *   fun Int.increment() = Either.right(this + 1)
 *   val result =
 *   //sampleStart
 *   IO.just(1).mapEither { it.increment() }
 *   //sampleEnd
 *   println(result.unsafeRunSyncEither())
 * }
 * ```
 */
fun <E, A, B> IO<E, A>.mapEither(f: (A) -> EitherOf<E, B>): IO<E, B> =
  map(f).flattenEither()

/**
 * Transform, as a suspend effect, the value of an [IO] into an [Either] and consequently flatten into an [IO]
 *
 * @return success when [f] results in [Either.Right] or error when [f] results in [Either.Left]
 *
 * fun main(args: Array<String>) {
 *   suspend fun Int.increment() = Either.right(this + 1)
 *   val result =
 *   //sampleStart
 *   IO.just(1).effectMapEither { it.increment() }
 *   //sampleEnd
 *   println(result.unsafeRunSyncEither())
 * }
 * ```
 */
fun <E, A, B> IO<E, A>.effectMapEither(f: suspend (A) -> EitherOf<E, B>): IO<E, B> =
  effectMap(f).flattenEither()

/**
 * Compose this [IO] with another [IO] [fb] while ignoring the output.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.*
 *
 * fun main(args: Array<String>) {
 *   //sampleStart
 *   val result = IO.effect { println("Hello World!") }
 *     .followedBy(IO.effect { println("Goodbye World!") })
 *   //sampleEnd
 *   println(result.unsafeRunSync())
 * }
 * ```
 *
 * @see flatMap if you need to act on the output of the original [IO].
 */
fun <E, A, B, E2 : E> IOOf<E, A>.followedBy(fb: IOOf<E2, B>): IO<E2, B> =
  flatMap { fb }

/**
 * Given both the value and the function are within [IO], **ap**ply the function to the value.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.*
 *
 * fun main() {
 *   //sampleStart
 *   val someF: IO<Nothing, (Int) -> Long> = IO.just { i: Int -> i.toLong() + 1 }
 *   val a = IO.just(3).ap(someF)
 *   val b = IO.raiseException<Int>(RuntimeException("Boom")).ap(someF)
 *   val c = IO.just(3).ap(IO.raiseException<(Int) -> Long>(RuntimeException("Boom")))
 *   //sampleEnd
 *   println("a: $a, b: $b, c: $c")
 * }
 * ```
 */
fun <E, A, B> IOOf<E, A>.ap(ff: IOOf<E, (A) -> B>): IO<E, B> =
  flatMap { a -> ff.fix().map { it(a) } }

fun <E, A, E2, B : A> IOOf<E, A>.flatMapLeft(f: (E) -> IOOf<E2, A>): IO<E2, A> =
  when (val bio = fix()) {
    is IO.RaiseError -> f(bio.error).fix()
    is IO.Pure,
    is IO.RaiseException -> bio as IO<E2, B>
    else -> IO.Bind(bio, IOFrame.Companion.MapError(f))
  }

fun <E, A, E2> IOOf<E, A>.mapError(f: (E) -> E2): IO<E2, A> =
  flatMapLeft { e -> IO.RaiseError(f(e)) }

fun <E, A, E2, B> IOOf<E, A>.bimap(fe: (E) -> E2, fa: (A) -> B): IO<E2, B> =
  mapError(fe).map(fa)

/**
 * Meant for specifying tasks with safe resource acquisition and release in the face of errors and interruption.
 * It would be the the equivalent of `try/catch/finally` statements in mainstream imperative languages for resource
 * acquisition and release.
 *
 * @param release is the action that's supposed to release the allocated resource after `use` is done, irregardless
 * of its exit condition.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.IO
 * import arrow.fx.unsafeRunSync
 *
 * class File(url: String) {
 *   fun open(): File = this
 *   fun close(): Unit {}
 *   override fun toString(): String = "This file contains some interesting content!"
 * }
 *
 * fun openFile(uri: String): IO<Nothing, File> = IO { File(uri).open() }
 * fun closeFile(file: File): IO<Nothing, Unit> = IO { file.close() }
 * fun fileToString(file: File): IO<Nothing, String> = IO { file.toString() }
 *
 * fun main(args: Array<String>) {
 *   //sampleStart
 *   val safeComputation = openFile("data.json").bracket({ file: File -> closeFile(file) }, { file -> fileToString(file) })
 *   //sampleEnd
 *   println(safeComputation.unsafeRunSync())
 * }
 * ```
 */
fun <E, A, B> IOOf<E, A>.bracket(release: (A) -> IOOf<E, Unit>, use: (A) -> IOOf<E, B>): IO<E, B> =
  bracketCase({ a, _ -> release(a) }, use)

/**
 * A way to safely acquire a resource and release in the face of errors and cancellation.
 * It uses [ExitCase] to distinguish between different exit cases when releasing the acquired resource.
 *
 * [Bracket] exists out of a three stages:
 *   1. acquisition
 *   2. consumption
 *   3. releasing
 *
 * 1. Resource acquisition is **NON CANCELLABLE**.
 *   If resource acquisition fails, meaning no resource was actually successfully acquired then we short-circuit the effect.
 *   Reason being, we cannot [release] what we did not `acquire` first. Same reason we cannot call [use].
 *   If it is successful we pass the result to stage 2 [use].
 *
 * 2. Resource consumption is like any other [IO] effect. The key difference here is that it's wired in such a way that
 *   [release] **will always** be called either on [ExitCase.Cancelled], [ExitCase.Error] or [ExitCase.Completed].
 *   If it failed than the resulting [IO] from [bracketCase] will be `IO.raiseError(e)`, otherwise the result of [use].
 *
 * 3. Resource releasing is **NON CANCELLABLE**, otherwise it could result in leaks.
 *   In the case it throws the resulting [IO] will be either the error or a composed error if one occurred in the [use] stage.
 *
 * @param use is the action to consume the resource and produce an [IO] with the result.
 * Once the resulting [IO] terminates, either successfully, error or disposed,
 * the [release] function will run to clean up the resources.
 *
 * @param release the allocated resource after the resulting [IO] of [use] is terminates.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.*
 * import arrow.fx.typeclasses.ExitCase
 * import arrow.fx.unsafeRunSync
 *
 * class File(url: String) {
 *   fun open(): File = this
 *   fun close(): Unit {}
 *   fun content(): IO<Nothing, String> =
 *     IO.just("This file contains some interesting content!")
 * }
 *
 * fun openFile(uri: String): IO<Nothing, File> = IO { File(uri).open() }
 * fun closeFile(file: File): IO<Nothing, Unit> = IO { file.close() }
 *
 * fun main(args: Array<String>) {
 *   //sampleStart
 *   val safeComputation = openFile("data.json").bracketCase(
 *     release = { file, exitCase ->
 *       when (exitCase) {
 *         is ExitCase.Completed -> { /* do something */ }
 *         is ExitCase.Cancelled -> { /* do something */ }
 *         is ExitCase.Error -> { /* do something */ }
 *       }
 *       closeFile(file)
 *     },
 *     use = { file -> file.content() }
 *   )
 *   //sampleEnd
 *   println(safeComputation.unsafeRunSync())
 * }
 *  ```
 */
fun <A, E, B> IOOf<E, A>.bracketCase(release: (A, ExitCase2<E>) -> IOOf<E, Unit>, use: (A) -> IOOf<E, B>): IO<E, B> =
  IOBracket(this, release, use)

/**
 * Executes the given [finalizer] when the source is finished, either in success or in error, or if cancelled.
 *
 * As best practice, prefer [bracket] for the acquisition and release of resources.
 *
 * @see [guaranteeCase] for the version that can discriminate between termination conditions
 * @see [bracket] for the more general operation
 */
fun <E, A> IOOf<E, A>.guarantee(finalizer: IOOf<E, Unit>): IO<E, A> =
  guaranteeCase { finalizer }

/**
 * Executes the given [finalizer] when the source is cancelled, allowing registering a cancellation token.
 *
 * Useful for wiring cancellation tokens between fibers, building inter-op with other effect systems or testing.
 */
fun <E, A> IOOf<E, A>.onCancel(finalizer: IOOf<E, Unit>): IO<E, A> =
  guaranteeCase { case ->
    when (case) {
      ExitCase2.Cancelled -> finalizer
      else -> IO.unit
    }
  }

fun <E, A> IOOf<E, A>.onError(finalizer: (E) -> IOOf<E, Unit>): IO<E, A> =
  guaranteeCase { case ->
    when (case) {
      is ExitCase2.Error<E> -> finalizer(case.error)
      else -> IO.unit
    }
  }

/**
 * Executes the given [finalizer] when the source is finishes with an error.
 */
fun <E, A> IOOf<E, A>.onException(finalizer: (Throwable) -> IOOf<E, Unit>): IO<E, A> =
  guaranteeCase { case ->
    when (case) {
      is ExitCase2.Exception -> finalizer(case.exception)
      else -> IO.unit
    }
  }

/**
 * Create a new [IO] that upon execution starts the receiver [IO] within a [Fiber] on [ctx].
 *
 * ```kotlin:ank:playground
 * import arrow.fx.*
 * import arrow.fx.extensions.fx
 * import arrow.fx.extensions.io.dispatchers.dispatchers
 *
 * fun main(args: Array<String>) {
 *   //sampleStart
 *   val result = IO.fx<Nothing, Unit> {
 *     val (join, cancel) = !IO.effect {
 *       println("Hello from a fiber on ${Thread.currentThread().name}")
 *     }.fork(IO.dispatchers<Nothing>().io())
 *   }
 *   //sampleEnd
 *   result.unsafeRunSync()
 * }
 * ```
 *
 * @receiver [IO] to execute on [ctx] within a new suspended [IO].
 * @param ctx [CoroutineContext] to execute the source [IO] on.
 * @return [IO] with suspended execution of source [IO] on context [ctx].
 */
fun <E, A> IOOf<E, A>.fork(ctx: CoroutineContext = IO.dispatchers<Nothing>().default()): IO<E, Fiber<IOPartialOf<E>, A>> =
  IO.async { cb ->
    val promise = UnsafePromise<E, A>()
    // A new IOConnection, because its cancellation is now decoupled from our current one.
    val conn = IOConnection()
    IORunLoop.startCancellable(IOForkedStart(this, ctx), conn, promise::complete)
    cb(IOResult.Success(IOFiber(promise, conn)))
  }

fun <A> IOOf<Nothing, A>.unsafeRunSync(): A =
  fix().unsafeRunSyncEither()
    .fold({ throw IONothingYieldsError }, ::identity)

fun <A> IOOf<Nothing, A>.unsafeRunAsync(f: (Either<Throwable, A>) -> Unit): Unit =
  fix().unsafeRunAsyncEither { result ->
    when (result) {
      is IOResult.Success -> f(Right(result.value))
      is IOResult.Exception -> f(Left(result.exception))
      is IOResult.Error -> result.error
    }
  }

fun <A> IOOf<Nothing, A>.unsafeRunAsyncCancellable(onCancel: OnCancel = Silent, cb: (Either<Throwable, A>) -> Unit): Disposable =
  fix().unsafeRunAsyncCancellableEither(onCancel) { result ->
    when (result) {
      is IOResult.Success -> cb(Right(result.value))
      is IOResult.Exception -> cb(Left(result.exception))
      is IOResult.Error -> result.error
    }
  }

fun <A, B> IOOf<Nothing, A>.repeat(schedule: Schedule<IOPartialOf<Nothing>, A, B>): IO<Nothing, B> =
  repeat(IO.concurrent(), schedule).fix()

fun <A, B> IOOf<Nothing, A>.retry(schedule: Schedule<IOPartialOf<Nothing>, Throwable, B>): IO<Nothing, A> =
  retry(IO.concurrent(), schedule).fix()

fun <E, A> IOOf<E, A>.void(): IO<E, Unit> =
  fix().map(mapUnit)

internal object IONothingYieldsError : ArrowInternalException() {
  override fun fillInStackTrace(): Throwable = this
}

internal object IOTimedResultedException : ArrowInternalException() {
  override fun fillInStackTrace(): Throwable = this
}
