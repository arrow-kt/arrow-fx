package arrow.fx.reactor

import arrow.core.Either
import arrow.core.Either.Right
import arrow.core.Left
import arrow.core.NonFatal
import arrow.fx.internal.AtomicBooleanW
import arrow.fx.internal.AtomicRefW
import arrow.core.nonFatalOrThrow
import arrow.fx.internal.Platform
import arrow.fx.internal.Platform.onceOnly
import arrow.fx.reactor.CoroutineContextReactorScheduler.asScheduler
import arrow.fx.typeclasses.CancelToken
import arrow.fx.typeclasses.Disposable
import arrow.fx.typeclasses.ExitCase
import reactor.core.publisher.Mono
import reactor.core.publisher.MonoSink
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

@Deprecated(DeprecateReactor)
class ForMonoK private constructor() {
  companion object
}

@Deprecated(DeprecateReactor)
typealias MonoKOf<A> = arrow.Kind<ForMonoK, A>

@Suppress("UNCHECKED_CAST", "NOTHING_TO_INLINE")
@Deprecated(DeprecateReactor)
inline fun <A> MonoKOf<A>.fix(): MonoK<A> =
  this as MonoK<A>

@Deprecated(DeprecateReactor)
fun <A> Mono<A>.k(): MonoK<A> = MonoK(this)

@Suppress("UNCHECKED_CAST")
@Deprecated(DeprecateReactor)
fun <A> MonoKOf<A>.value(): Mono<A> =
  this.fix().mono as Mono<A>

@Deprecated(DeprecateReactor)
data class MonoK<out A>(val mono: Mono<out A>) : MonoKOf<A> {

  @Deprecated(DeprecateReactor)
  suspend fun suspended(): A? = suspendCoroutine { cont ->
    onceOnly(cont::resume).let { cb ->
      value().subscribe({ cb(it) }, cont::resumeWithException) { cb(null) }
    }
  }

  @Deprecated(DeprecateReactor)
  fun <B> map(f: (A) -> B): MonoK<B> =
    mono.map(f).k()

  @Deprecated(DeprecateReactor)
  fun <B> ap(fa: MonoKOf<(A) -> B>): MonoK<B> =
    flatMap { a -> fa.fix().map { ff -> ff(a) } }

  @Deprecated(DeprecateReactor)
  fun <B> flatMap(f: (A) -> MonoKOf<B>): MonoK<B> =
    mono.flatMap { f(it).fix().mono }.k()

  /**
   * A way to safely acquire a resource and release in the face of errors and cancellation.
   * It uses [ExitCase] to distinguish between different exit cases when releasing the acquired resource.
   *
   * @param use is the action to consume the resource and produce an [MonoK] with the result.
   * Once the resulting [MonoK] terminates, either successfully, error or disposed,
   * the [release] function will run to clean up the resources.
   *
   * @param release the allocated resource after the resulting [MonoK] of [use] is terminates.
   *
   * {: data-executable='true'}
   * ```kotlin:ank
   * import arrow.fx.*
   * import arrow.fx.reactor.*
   * import arrow.fx.typeclasses.ExitCase
   *
   * class File(url: String) {
   *   fun open(): File = this
   *   fun close(): Unit {}
   *   fun content(): MonoK<String> =
   *     MonoK.just("This file contains some interesting content!")
   * }
   *
   * fun openFile(uri: String): MonoK<File> = MonoK { File(uri).open() }
   * fun closeFile(file: File): MonoK<Unit> = MonoK { file.close() }
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
   *   println(safeComputation)
   * }
   *  ```
   */
  @Deprecated(DeprecateReactor)
  fun <B> bracketCase(use: (A) -> MonoKOf<B>, release: (A, ExitCase<Throwable>) -> MonoKOf<Unit>): MonoK<B> =
    MonoK(
      Mono.create<B> { sink ->
        val isCancelled = AtomicBooleanW(false)
        sink.onCancel { isCancelled.value = true }
        val a: A? = mono.block()
        if (a != null) {
          if (isCancelled.value) release(a, ExitCase.Cancelled).fix().mono.subscribe({}, sink::error)
          else try {
            sink.onDispose(
              use(a).fix()
                .flatMap { b -> release(a, ExitCase.Completed).fix().map { b } }
                .handleErrorWith { e -> release(a, ExitCase.Error(e)).fix().flatMap { MonoK.raiseError<B>(e) } }
                .mono
                .doOnCancel { release(a, ExitCase.Cancelled).fix().mono.subscribe({}, sink::error) }
                .subscribe(sink::success, sink::error)
            )
          } catch (e: Throwable) {
            if (NonFatal(e)) {
              release(a, ExitCase.Error(e)).fix().mono.subscribe(
                {
                  sink.error(e)
                },
                { e2 ->
                  sink.error(Platform.composeErrors(e, e2))
                }
              )
            } else {
              throw e
            }
          }
        } else sink.success(null)
      }
    )

  @Deprecated(DeprecateReactor)
  fun continueOn(ctx: CoroutineContext): MonoK<A> =
    mono.publishOn(ctx.asScheduler()).k()

  @Deprecated(DeprecateReactor)
  fun runAsync(cb: (Either<Throwable, A>) -> MonoKOf<Unit>): MonoK<Unit> =
    mono.flatMap { cb(Right(it)).value() }.onErrorResume { cb(Left(it)).value() }.k()

  @Deprecated(DeprecateReactor)
  fun runAsyncCancellable(cb: (Either<Throwable, A>) -> MonoKOf<Unit>): MonoK<Disposable> =
    Mono.fromCallable {
      val disposable: reactor.core.Disposable = runAsync(cb).value().subscribe()
      val dispose: Disposable = disposable::dispose
      dispose
    }.k()

  @Deprecated(DeprecateReactor)
  override fun equals(other: Any?): Boolean =
    when (other) {
      is MonoK<*> -> this.mono == other.mono
      is Mono<*> -> this.mono == other
      else -> false
    }

  @Deprecated(DeprecateReactor)
  override fun hashCode(): Int = mono.hashCode()

  companion object {
    @Deprecated(DeprecateReactor)
    fun <A> just(a: A): MonoK<A> =
      Mono.just(a).k()

    @Deprecated(DeprecateReactor)
    fun <A> raiseError(t: Throwable): MonoK<A> =
      Mono.error<A>(t).k()

    @Deprecated(DeprecateReactor)
    operator fun <A> invoke(fa: () -> A): MonoK<A> =
      defer { just(fa()) }

    @Deprecated(DeprecateReactor)
    fun <A> defer(fa: () -> MonoKOf<A>): MonoK<A> =
      Mono.defer { fa().value() }.k()

    /**
     * Constructor for wrapping uncancellable async operations.
     * It's safe to wrap unsafe operations in this constructor
     *
     * ```kotlin:ank:playground
     * import arrow.core.Either
     * import arrow.core.right
     * import arrow.fx.reactor.MonoK
     * import arrow.fx.reactor.value
     *
     * class Resource {
     *   fun asyncRead(f: (String) -> Unit): Unit = f("Some value of a resource")
     *   fun close(): Unit = Unit
     * }
     *
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   val result = MonoK.async { cb: (Either<Throwable, String>) -> Unit ->
     *     val resource = Resource()
     *     resource.asyncRead { value -> cb(value.right()) }
     *   }
     *   //sampleEnd
     *   result.value().subscribe(::println)
     * }
     * ```
     */
    @Deprecated(DeprecateReactor)
    fun <A> async(fa: ((Either<Throwable, A>) -> Unit) -> Unit): MonoK<A> =
      Mono.create<A> { sink ->
        fa { either: Either<Throwable, A> ->
          either.fold(
            {
              sink.error(it)
            },
            {
              sink.success(it)
            }
          )
        }
      }.k()

    @Deprecated(DeprecateReactor)
    fun <A> asyncF(fa: ((Either<Throwable, A>) -> Unit) -> MonoKOf<Unit>): MonoK<A> =
      Mono.create { sink: MonoSink<A> ->
        fa { either: Either<Throwable, A> ->
          either.fold(
            {
              sink.error(it)
            },
            {
              sink.success(it)
            }
          )
        }.fix().mono.subscribe({}, sink::error)
      }.k()

    /**
     * Creates a [MonoK] that'll wraps/runs a cancellable operation.
     *
     * ```kotlin:ank:playground
     * import arrow.core.*
     * import arrow.fx.rx2.*
     *
     * typealias Disposable = () -> Unit
     * class NetworkApi {
     *   fun async(f: (String) -> Unit): Disposable {
     *     f("Some value of a resource")
     *     return { Unit }
     *   }
     * }
     *
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   val result = SingleK.cancellable { cb: (Either<Throwable, String>) -> Unit ->
     *     val nw = NetworkApi()
     *     val disposable = nw.async { result -> cb(Right(result)) }
     *     SingleK { disposable.invoke() }
     *   }
     *   //sampleEnd
     *   result.value().subscribe(::println, ::println)
     * }
     * ```
     */
    @Deprecated(DeprecateReactor)
    fun <A> cancellable(fa: ((Either<Throwable, A>) -> Unit) -> CancelToken<ForMonoK>): MonoK<A> =
      Mono.create { sink: MonoSink<A> ->
        val cb = { either: Either<Throwable, A> ->
          either.fold(sink::error, sink::success)
        }

        val token = try {
          fa(cb)
        } catch (t: Throwable) {
          cb(Left(t.nonFatalOrThrow()))
          just(Unit)
        }

        sink.onDispose { token.value().subscribe({}, sink::error) }
      }.k()

    @Deprecated("Renaming this api for consistency", ReplaceWith("cancellable(fa)"))
    fun <A> cancelable(fa: ((Either<Throwable, A>) -> Unit) -> CancelToken<ForMonoK>): MonoK<A> =
      cancellable(fa)

    @Deprecated("Renaming this api for consistency", ReplaceWith("cancellableF(fa)"))
    fun <A> cancelableF(fa: ((Either<Throwable, A>) -> Unit) -> MonoKOf<CancelToken<ForMonoK>>): MonoK<A> =
      cancellableF(fa)

    @Deprecated(DeprecateReactor)
    fun <A> cancellableF(fa: ((Either<Throwable, A>) -> Unit) -> MonoKOf<CancelToken<ForMonoK>>): MonoK<A> =
      Mono.create { sink: MonoSink<A> ->
        val cb = { either: Either<Throwable, A> ->
          either.fold(sink::error, sink::success)
        }

        val fa2 = try {
          fa(cb)
        } catch (t: Throwable) {
          cb(Left(t.nonFatalOrThrow()))
          just(just(Unit))
        }

        val cancelOrToken = AtomicRefW<Either<Unit, CancelToken<ForMonoK>>?>(null)
        val disp = fa2.value().subscribe(
          { token ->
            val cancel = cancelOrToken.getAndSet(Right(token))
            cancel?.fold(
              {
                token.value().subscribe({}, sink::error)
              },
              { Unit }
            )
          },
          sink::error
        )

        sink.onDispose {
          disp.dispose()
          val token = cancelOrToken.getAndSet(Left(Unit))
          token?.fold(
            {},
            {
              it.value().subscribe({}, sink::error)
            }
          )
        }
      }.k()

    @Deprecated(DeprecateReactor)
    tailrec fun <A, B> tailRecM(a: A, f: (A) -> MonoKOf<Either<A, B>>): MonoK<B> {
      val either = f(a).value().block()
      return when (either) {
        is Either.Left -> tailRecM(either.a, f)
        is Either.Right -> Mono.just(either.b).k()
      }
    }
  }
}

/**
 * Runs the [MonoK] asynchronously and then runs the cb.
 * Catches all errors that may be thrown in await. Errors from cb will still throw as expected.
 *
 * ```kotlin:ank:playground
 * import arrow.core.Either
 * import arrow.fx.reactor.*
 *
 * fun main(args: Array<String>) {
 *   //sampleStart
 *   MonoK.just(1).unsafeRunAsync { either: Either<Throwable, Int> ->
 *     either.fold({ t: Throwable ->
 *       println(t)
 *     }, { i: Int ->
 *       println("Finished with $i")
 *     })
 *   }
 *   //sampleEnd
 * }
 * ```
 */
@Deprecated(DeprecateReactor)
fun <A> MonoKOf<A>.unsafeRunAsync(cb: (Either<Throwable, A>) -> Unit): Unit =
  value().subscribe({ cb(arrow.core.Right(it)) }, { cb(arrow.core.Left(it)) }).let { }

/**
 * Runs this [MonoKOf] with [Mono.block]. Does not handle errors at all, rethrowing them if they happen.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.reactor.*
 *
 * fun main(args: Array<String>) {
 *   //sampleStart
 *   val result: MonoK<String> = MonoK.raiseError<String>(Exception("BOOM"))
 *   //sampleEnd
 *   println(result.unsafeRunSync())
 * }
 * ```
 */
@Deprecated(DeprecateReactor)
fun <A> MonoKOf<A>.unsafeRunSync(): A? =
  value().block()

@Deprecated(DeprecateReactor)
fun <A> MonoK<A>.handleErrorWith(function: (Throwable) -> MonoK<A>): MonoK<A> =
  value().onErrorResume { t: Throwable -> function(t).value() }.k()
