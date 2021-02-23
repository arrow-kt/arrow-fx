package arrow.fx.reactor

import arrow.Kind
import arrow.core.Either
import arrow.core.Eval
import arrow.core.Left
import arrow.core.NonFatal
import arrow.core.Option
import arrow.core.Right
import arrow.core.identity
import arrow.fx.internal.AtomicRefW
import arrow.core.nonFatalOrThrow
import arrow.fx.internal.Platform
import arrow.fx.reactor.CoroutineContextReactorScheduler.asScheduler
import arrow.fx.typeclasses.CancelToken
import arrow.fx.typeclasses.Disposable
import arrow.fx.typeclasses.ExitCase
import arrow.typeclasses.Applicative
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink
import kotlin.coroutines.CoroutineContext

@Deprecated(DeprecateReactor)
class ForFluxK private constructor() {
  companion object
}

@Deprecated(DeprecateReactor)
typealias FluxKOf<A> = arrow.Kind<ForFluxK, A>

@Suppress("UNCHECKED_CAST", "NOTHING_TO_INLINE")
@Deprecated(DeprecateReactor)
inline fun <A> FluxKOf<A>.fix(): FluxK<A> =
  this as FluxK<A>

@Deprecated(DeprecateReactor)
fun <A> Flux<A>.k(): FluxK<A> = FluxK(this)

@Suppress("UNCHECKED_CAST")
@Deprecated(DeprecateReactor)
fun <A> FluxKOf<A>.value(): Flux<A> =
  this.fix().flux as Flux<A>

@Suppress("NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")
@Deprecated(DeprecateReactor)
data class FluxK<out A>(val flux: Flux<out A>) : FluxKOf<A> {

  @Deprecated(DeprecateReactor)
  fun <B> map(f: (A) -> B): FluxK<B> =
    flux.map(f).k()

  @Deprecated(DeprecateReactor)
  fun <B> ap(fa: FluxKOf<(A) -> B>): FluxK<B> =
    flatMap { a -> fa.fix().map { ff -> ff(a) } }

  @Deprecated(DeprecateReactor)
  fun <B> flatMap(f: (A) -> FluxKOf<B>): FluxK<B> =
    flux.flatMap { f(it).fix().flux }.k()

  /**
   * A way to safely acquire a resource and release in the face of errors and cancellation.
   * It uses [ExitCase] to distinguish between different exit cases when releasing the acquired resource.
   *
   * @param use is the action to consume the resource and produce an [FluxK] with the result.
   * Once the resulting [FluxK] terminates, either successfully, error or disposed,
   * the [release] function will run to clean up the resources.
   *
   * @param release the allocated resource after the resulting [FluxK] of [use] is terminates.
   *
   * {: data-executable='true'}
   * ```kotlin:ank
   * import reactor.core.publisher.Flux
   * import arrow.fx.reactor.*
   * import arrow.fx.typeclasses.ExitCase
   *
   * class File(url: String) {
   *   fun open(): File = this
   *   fun close(): Unit {}
   *   fun content(): FluxK<String> =
   *     Flux.just("This", "file", "contains", "some", "interesting", "content!").k()
   * }
   *
   * fun openFile(uri: String): FluxK<File> = FluxK { File(uri).open() }
   * fun closeFile(file: File): FluxK<Unit> = FluxK { file.close() }
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
  fun <B> bracketCase(use: (A) -> FluxKOf<B>, release: (A, ExitCase<Throwable>) -> FluxKOf<Unit>): FluxK<B> =
    FluxK(
      Flux.create<B> { sink ->
        flux.subscribe(
          { a ->
            if (sink.isCancelled) release(a, ExitCase.Cancelled).fix().flux.subscribe({}, sink::error)
            else try {
              sink.onDispose(
                use(a).fix()
                  .flatMap { b -> release(a, ExitCase.Completed).fix().map { b } }
                  .handleErrorWith { e -> release(a, ExitCase.Error(e)).fix().flatMap { FluxK.raiseError<B>(e) } }
                  .flux
                  .doOnCancel { release(a, ExitCase.Cancelled).fix().flux.subscribe({}, sink::error) }
                  .subscribe(
                    { sink.next(it) }, sink::error, { },
                    {
                      sink.onRequest(it::request)
                    }
                  )
              )
            } catch (e: Throwable) {
              if (NonFatal(e)) {
                release(a, ExitCase.Error(e)).fix().flux.subscribe(
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
          },
          sink::error, sink::complete
        )
      }
    )

  @Deprecated(DeprecateReactor)
  fun <B> concatMap(f: (A) -> FluxKOf<B>): FluxK<B> =
    flux.concatMap { f(it).fix().flux }.k()

  @Deprecated(DeprecateReactor)
  fun <B> switchMap(f: (A) -> FluxKOf<B>): FluxK<B> =
    flux.switchMap { f(it).fix().flux }.k()

  @Deprecated(DeprecateReactor)
  fun <B> foldLeft(b: B, f: (B, A) -> B): B = flux.reduce(b, f).block()

  @Deprecated(DeprecateReactor)
  fun <B> foldRight(lb: Eval<B>, f: (A, Eval<B>) -> Eval<B>): Eval<B> {
    fun loop(fa_p: FluxK<A>): Eval<B> = when {
      fa_p.flux.hasElements().map { !it }.block() -> lb
      else -> f(fa_p.flux.blockFirst(), Eval.defer { loop(fa_p.flux.skip(1).k()) })
    }

    return Eval.defer { loop(this) }
  }

  @Deprecated(DeprecateReactor)
  fun <G, B> traverse(GA: Applicative<G>, f: (A) -> Kind<G, B>): Kind<G, FluxK<B>> =
    foldRight(Eval.always { GA.just(Flux.empty<B>().k()) }) { a, eval ->
      GA.run { f(a).map2Eval(eval) { Flux.concat(Flux.just<B>(it.a), it.b.flux).k() } }
    }.value()

  @Deprecated(DeprecateReactor)
  fun continueOn(ctx: CoroutineContext): FluxK<A> =
    flux.publishOn(ctx.asScheduler()).k()

  @Deprecated(DeprecateReactor)
  fun runAsync(cb: (Either<Throwable, A>) -> FluxKOf<Unit>): FluxK<Unit> =
    flux.flatMap { cb(Right(it)).value() }.onErrorResume { cb(Left(it)).value() }.k()

  @Deprecated(DeprecateReactor)
  fun runAsyncCancellable(cb: (Either<Throwable, A>) -> FluxKOf<Unit>): FluxK<Disposable> =
    Flux.defer {
      val disposable: reactor.core.Disposable = runAsync(cb).value().subscribe()
      val dispose: Disposable = { disposable.dispose() }
      Flux.just(dispose)
    }.k()

  @Deprecated(DeprecateReactor)
  override fun equals(other: Any?): Boolean =
    when (other) {
      is FluxK<*> -> this.flux == other.flux
      is Flux<*> -> this.flux == other
      else -> false
    }

  @Deprecated(DeprecateReactor)
  fun <B> filterMap(f: (A) -> Option<B>): FluxK<B> =
    flux.flatMap { a ->
      f(a).fold({ Flux.empty<B>() }, { b -> Flux.just(b) })
    }.k()

  @Deprecated(DeprecateReactor)
  override fun hashCode(): Int = flux.hashCode()

  companion object {
    @Deprecated(DeprecateReactor)
    fun <A> just(a: A): FluxK<A> =
      Flux.just(a).k()

    @Deprecated(DeprecateReactor)
    fun <A> raiseError(t: Throwable): FluxK<A> =
      Flux.error<A>(t).k()

    @Deprecated(DeprecateReactor)
    operator fun <A> invoke(fa: () -> A): FluxK<A> =
      defer { just(fa()) }

    @Deprecated(DeprecateReactor)
    fun <A> defer(fa: () -> FluxKOf<A>): FluxK<A> =
      Flux.defer { fa().value() }.k()

    /**
     * Creates a [FluxK] that'll run [FluxKProc].
     *
     * ```kotlin:ank:playground
     * import arrow.core.Either
     * import arrow.core.right
     * import arrow.fx.reactor.FluxK
     * import arrow.fx.reactor.value
     *
     * class Resource {
     *   fun asyncRead(f: (String) -> Unit): Unit = f("Some value of a resource")
     *   fun close(): Unit = Unit
     * }
     *
     * fun main(args: Array<String>) {
     *   //sampleStart
     *   val result = FluxK.async { cb: (Either<Throwable, String>) -> Unit ->
     *     val resource = Resource()
     *     resource.asyncRead { value -> cb(value.right()) }
     *   }
     *   //sampleEnd
     *   result.value().subscribe(::println)
     * }
     * ```
     */
    @Deprecated(DeprecateReactor)
    fun <A> async(fa: ((Either<Throwable, A>) -> Unit) -> Unit): FluxK<A> =
      Flux.create<A> { sink ->
        fa { callback: Either<Throwable, A> ->
          callback.fold(
            {
              sink.error(it)
            },
            {
              sink.next(it)
              sink.complete()
            }
          )
        }
      }.k()

    @Deprecated(DeprecateReactor)
    fun <A> asyncF(fa: ((Either<Throwable, A>) -> Unit) -> FluxKOf<Unit>): FluxK<A> =
      Flux.create { sink: FluxSink<A> ->
        fa { callback: Either<Throwable, A> ->
          callback.fold(
            {
              sink.error(it)
            },
            {
              sink.next(it)
              sink.complete()
            }
          )
        }.fix().flux.subscribe({}, sink::error)
      }.k()

    @Deprecated("Renaming this api for consistency", ReplaceWith("cancellable(fa)"))
    fun <A> cancelable(fa: ((Either<Throwable, A>) -> Unit) -> CancelToken<ForFluxK>): FluxK<A> =
      cancellable(fa)

    @Deprecated(DeprecateReactor)
    fun <A> cancellable(fa: ((Either<Throwable, A>) -> Unit) -> CancelToken<ForFluxK>): FluxK<A> =
      Flux.create<A> { sink ->
        val token = fa { either: Either<Throwable, A> ->
          either.fold(
            { e ->
              sink.error(e)
            },
            { a ->
              sink.next(a)
              sink.complete()
            }
          )
        }
        sink.onDispose { token.value().subscribe({}, sink::error) }
      }.k()

    @Deprecated("Renaming this api for consistency", ReplaceWith("cancellableF(fa)"))
    fun <A> cancelableF(fa: ((Either<Throwable, A>) -> Unit) -> FluxKOf<CancelToken<ForFluxK>>): FluxK<A> =
      cancellableF(fa)

    @Deprecated(DeprecateReactor)
    fun <A> cancellableF(fa: ((Either<Throwable, A>) -> Unit) -> FluxKOf<CancelToken<ForFluxK>>): FluxK<A> =
      Flux.create<A> { sink ->
        val cb = { either: Either<Throwable, A> ->
          either.fold(
            { e ->
              sink.error(e)
            },
            { a ->
              sink.next(a)
              sink.complete()
            }
          )
        }

        val fa2 = try {
          fa(cb)
        } catch (t: Throwable) {
          cb(Left(t.nonFatalOrThrow()))
          just(just(Unit))
        }

        val cancelOrToken = AtomicRefW<Either<Unit, CancelToken<ForFluxK>>?>(null)
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
    tailrec fun <A, B> tailRecM(a: A, f: (A) -> FluxKOf<Either<A, B>>): FluxK<B> {
      val either = f(a).value().blockFirst()
      return when (either) {
        is Either.Left -> tailRecM(either.a, f)
        is Either.Right -> Flux.just(either.b).k()
      }
    }
  }
}

@Deprecated(DeprecateReactor)
fun <A, G> FluxKOf<Kind<G, A>>.sequence(GA: Applicative<G>): Kind<G, FluxK<A>> =
  fix().traverse(GA, ::identity)

@Deprecated(DeprecateReactor)
fun <A> FluxKOf<A>.handleErrorWith(function: (Throwable) -> FluxK<A>): FluxK<A> =
  value().onErrorResume { t: Throwable -> function(t).value() }.k()
