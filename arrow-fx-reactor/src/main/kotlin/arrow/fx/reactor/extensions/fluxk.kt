package arrow.fx.reactor.extensions

import arrow.Kind
import arrow.core.Either
import arrow.core.Eval
import arrow.core.Option
import arrow.core.Tuple2
import arrow.core.Tuple3
import arrow.core.internal.AtomicBooleanW
import arrow.extension
import arrow.fx.RacePair
import arrow.fx.RaceTriple
import arrow.fx.Timer
import arrow.fx.reactor.CoroutineContextReactorScheduler.asScheduler
import arrow.fx.reactor.FluxK
import arrow.fx.reactor.FluxKOf
import arrow.fx.reactor.ForFluxK
import arrow.fx.reactor.extensions.fluxk.async.async
import arrow.fx.reactor.extensions.fluxk.dispatchers.dispatchers
import arrow.fx.reactor.extensions.fluxk.monad.monad
import arrow.fx.reactor.extensions.fluxk.monadError.monadError
import arrow.fx.reactor.fix
import arrow.fx.reactor.k
import arrow.fx.reactor.value
import arrow.fx.typeclasses.Async
import arrow.fx.typeclasses.Bracket
import arrow.fx.typeclasses.CancelToken
import arrow.fx.typeclasses.Concurrent
import arrow.fx.typeclasses.ConcurrentEffect
import arrow.fx.typeclasses.ConcurrentSyntax
import arrow.fx.typeclasses.Dispatchers
import arrow.fx.typeclasses.Disposable
import arrow.fx.typeclasses.Duration
import arrow.fx.typeclasses.Effect
import arrow.fx.typeclasses.ExitCase
import arrow.fx.typeclasses.Fiber
import arrow.fx.typeclasses.MonadDefer
import arrow.fx.typeclasses.Proc
import arrow.fx.typeclasses.ProcF
import arrow.typeclasses.Applicative
import arrow.typeclasses.ApplicativeError
import arrow.typeclasses.Foldable
import arrow.typeclasses.Functor
import arrow.typeclasses.FunctorFilter
import arrow.typeclasses.Monad
import arrow.typeclasses.MonadError
import arrow.typeclasses.MonadFilter
import arrow.typeclasses.MonadThrow
import arrow.typeclasses.Traverse
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink
import reactor.core.publisher.Mono
import reactor.core.publisher.ReplayProcessor
import reactor.core.publisher.toFlux
import reactor.core.Disposable as ReactorDisposable
import kotlin.coroutines.CoroutineContext
import arrow.fx.reactor.handleErrorWith as fluxHandleErrorWith

@extension
interface FluxKFunctor : Functor<ForFluxK> {
  override fun <A, B> FluxKOf<A>.map(f: (A) -> B): FluxK<B> =
    fix().map(f)
}

@extension
interface FluxKApplicative : Applicative<ForFluxK> {
  override fun <A> just(a: A): FluxK<A> =
    FluxK.just(a)

  override fun <A, B> FluxKOf<A>.ap(ff: FluxKOf<(A) -> B>): FluxK<B> =
    fix().ap(ff)

  override fun <A, B> FluxKOf<A>.map(f: (A) -> B): FluxK<B> =
    fix().map(f)

  override fun <A, B> Kind<ForFluxK, A>.apEval(ff: Eval<Kind<ForFluxK, (A) -> B>>): Eval<Kind<ForFluxK, B>> =
    Eval.now(fix().ap(FluxK.defer { ff.value() }))
}

@extension
interface FluxKMonad : Monad<ForFluxK>, FluxKApplicative {
  override fun <A, B> FluxKOf<A>.ap(ff: FluxKOf<(A) -> B>): FluxK<B> =
    fix().ap(ff)

  override fun <A, B> FluxKOf<A>.flatMap(f: (A) -> FluxKOf<B>): FluxK<B> =
    fix().flatMap(f)

  override fun <A, B> FluxKOf<A>.map(f: (A) -> B): FluxK<B> =
    fix().map(f)

  override fun <A, B> tailRecM(a: A, f: kotlin.Function1<A, FluxKOf<arrow.core.Either<A, B>>>): FluxK<B> =
    FluxK.tailRecM(a, f)

  override fun <A, B> Kind<ForFluxK, A>.apEval(ff: Eval<Kind<ForFluxK, (A) -> B>>): Eval<Kind<ForFluxK, B>> =
    Eval.now(fix().ap(FluxK.defer { ff.value() }))
}

@extension
interface FluxKFoldable : Foldable<ForFluxK> {
  override fun <A, B> FluxKOf<A>.foldLeft(b: B, f: (B, A) -> B): B =
    fix().foldLeft(b, f)

  override fun <A, B> FluxKOf<A>.foldRight(lb: Eval<B>, f: (A, Eval<B>) -> Eval<B>): arrow.core.Eval<B> =
    fix().foldRight(lb, f)
}

@extension
interface FluxKTraverse : Traverse<ForFluxK> {
  override fun <A, B> FluxKOf<A>.map(f: (A) -> B): FluxK<B> =
    fix().map(f)

  override fun <G, A, B> FluxKOf<A>.traverse(AP: Applicative<G>, f: (A) -> Kind<G, B>): Kind<G, FluxK<B>> =
    fix().traverse(AP, f)

  override fun <A, B> FluxKOf<A>.foldLeft(b: B, f: (B, A) -> B): B =
    fix().foldLeft(b, f)

  override fun <A, B> FluxKOf<A>.foldRight(lb: Eval<B>, f: (A, Eval<B>) -> Eval<B>): arrow.core.Eval<B> =
    fix().foldRight(lb, f)
}

@extension
interface FluxKApplicativeError :
  ApplicativeError<ForFluxK, Throwable>,
  FluxKApplicative {
  override fun <A> raiseError(e: Throwable): FluxK<A> =
    FluxK.raiseError(e)

  override fun <A> FluxKOf<A>.handleErrorWith(f: (Throwable) -> FluxKOf<A>): FluxK<A> =
    fix().fluxHandleErrorWith { f(it).fix() }
}

@extension
interface FluxKMonadError :
  MonadError<ForFluxK, Throwable>,
  FluxKMonad {
  override fun <A> raiseError(e: Throwable): FluxK<A> =
    FluxK.raiseError(e)

  override fun <A> FluxKOf<A>.handleErrorWith(f: (Throwable) -> FluxKOf<A>): FluxK<A> =
    fix().fluxHandleErrorWith { f(it).fix() }
}

@extension
interface FluxKMonadThrow : MonadThrow<ForFluxK>, FluxKMonadError

@extension
interface FluxKBracket : Bracket<ForFluxK, Throwable>, FluxKMonadThrow {
  override fun <A, B> FluxKOf<A>.bracketCase(release: (A, ExitCase<Throwable>) -> Kind<ForFluxK, Unit>, use: (A) -> FluxKOf<B>): FluxK<B> =
    fix().bracketCase({ use(it) }, { a, e -> release(a, e) })

  override fun <A> Kind<ForFluxK, A>.guaranteeCase(finalizer: (ExitCase<Throwable>) -> Kind<ForFluxK, Unit>): Kind<ForFluxK, A> =
    fix().guaranteeCase(finalizer)
}

@extension
interface FluxKMonadDefer :
  MonadDefer<ForFluxK>,
  FluxKBracket {
  override fun <A> defer(fa: () -> FluxKOf<A>): FluxK<A> =
    FluxK.defer(fa)
}

@extension
interface FluxKAsync :
  Async<ForFluxK>,
  FluxKMonadDefer {
  override fun <A> async(fa: Proc<A>): FluxK<A> =
    FluxK.async(fa)

  override fun <A> asyncF(k: ProcF<ForFluxK, A>): FluxK<A> =
    FluxK.asyncF(k)

  override fun <A> FluxKOf<A>.continueOn(ctx: CoroutineContext): FluxK<A> =
    fix().continueOn(ctx)
}

interface FluxKConcurrent : Concurrent<ForFluxK>, FluxKAsync {

  override fun <A> Kind<ForFluxK, A>.fork(coroutineContext: CoroutineContext): FluxK<Fiber<ForFluxK, A>> =
    fix().fork(coroutineContext)

  override fun <A, B> parTupledN(ctx: CoroutineContext, fa: FluxKOf<A>, fb: FluxKOf<B>): FluxK<Tuple2<A, B>> =
    fa.value().zipWith(fb.value(), tupled()).subscribeOn(ctx.asScheduler()).k()

  override fun <A, B, C> parTupledN(ctx: CoroutineContext, fa: FluxKOf<A>, fb: FluxKOf<B>, fc: FluxKOf<C>): FluxK<Tuple3<A, B, C>> =
    Flux.zip(fa.value(), fb.value(), fc.value())
      .map { Tuple3(it.t1, it.t2, it.t3) }
      .subscribeOn(ctx.asScheduler()).k()

  override fun <A> cancellable(k: ((Either<Throwable, A>) -> Unit) -> CancelToken<ForFluxK>): FluxK<A> =
    FluxK.cancellable(k)

  override fun <A> cancellableF(k: ((Either<Throwable, A>) -> Unit) -> FluxKOf<CancelToken<ForFluxK>>): FluxK<A> =
    FluxK.cancellableF(k)

  override fun <A, B> CoroutineContext.racePair(fa: FluxKOf<A>, fb: FluxKOf<B>): FluxK<RacePair<ForFluxK, A, B>> =
    asScheduler().let { scheduler ->
      Flux.create<RacePair<ForFluxK, A, B>> { emitter ->
        val sa = ReplayProcessor.create<A>()
        val sb = ReplayProcessor.create<B>()

        val dda = fa.value()
          .subscribeOn(scheduler)
          .subscribe(sa::onNext, sa::onError)
        val ddb = fb.value()
          .subscribeOn(scheduler)
          .subscribe(sb::onNext, sb::onError)

        emitter.onCancel { dda.dispose(); ddb.dispose() }

        val ffa = Fiber(sa.k(), FluxK { dda.dispose() })
        val ffb = Fiber(sb.k(), FluxK { ddb.dispose() })

        sa.subscribe({
          emitter.next(RacePair.First(it, ffb))
        }, { e -> onError(ddb, emitter, e) }, emitter::complete)

        sb.subscribe({
          emitter.next(RacePair.Second(ffa, it))
        }, { e -> onError(dda, emitter, e) }, emitter::complete)
      }
        .subscribeOn(scheduler)
        .k()
    }

  fun <A, B> onError(
    other2: ReactorDisposable,
    sink: FluxSink<RacePair<ForFluxK, A, B>>,
    err: Throwable
  ) {
    other2.dispose()
    sink.error(err)
  }

  override fun <A, B, C> CoroutineContext.raceTriple(fa: FluxKOf<A>, fb: FluxKOf<B>, fc: FluxKOf<C>): FluxK<RaceTriple<ForFluxK, A, B, C>> =
    asScheduler().let { scheduler ->
      val active = AtomicBooleanW(true) // Prevent race-conditions in Success & Error'ing at same time.

      Flux.create<RaceTriple<ForFluxK, A, B, C>> { emitter ->
        val sa = ReplayProcessor.create<A>() // These should probably be `Queue` instead, put once, take once.
        val sb = ReplayProcessor.create<B>()
        val sc = ReplayProcessor.create<C>()

        val dda = fa.value()
          .subscribeOn(scheduler)
          .subscribe(sa::onNext, sa::onError)
        val ddb = fb.value()
          .subscribeOn(scheduler)
          .subscribe(sb::onNext, sb::onError)
        val ddc = fc.value()
          .subscribeOn(scheduler)
          .subscribe(sc::onNext, sc::onError)

        emitter.onCancel { dda.dispose(); ddb.dispose(); ddc.dispose() }

        val ffa = Fiber(sa.k(), FluxK { dda.dispose() })
        val ffb = Fiber(sb.k(), FluxK { ddb.dispose() })
        val ffc = Fiber(sc.k(), FluxK { ddc.dispose() })

        sa.subscribe({
          if (active.getAndSet(false)) emitter.next(RaceTriple.First(it, ffb, ffc))
        }, { e -> onError(active, ddb, ddc, emitter, sa, e) }, emitter::complete)

        sb.subscribe({
          if (active.getAndSet(false)) emitter.next(RaceTriple.Second(ffa, it, ffc))
        }, { e -> onError(active, dda, ddc, emitter, sb, e) }, emitter::complete)

        sc.subscribe({
          if (active.getAndSet(false)) emitter.next(RaceTriple.Third(ffa, ffb, it))
        }, { e -> onError(active, dda, ddb, emitter, sc, e) }, emitter::complete)
      }
        .subscribeOn(scheduler)
        .k()
    }

  private fun <A, B, C> onError(
    active: AtomicBooleanW,
    other2: ReactorDisposable,
    other3: ReactorDisposable,
    sink: FluxSink<RaceTriple<ForFluxK, A, B, C>>,
    processor: ReplayProcessor<*>,
    err: Throwable
  ): Unit = if (active.getAndSet(false)) {
    other2.dispose()
    other3.dispose()
    sink.error(err)
  } else {
    processor.onError(err)
  }
}

fun FluxK.Companion.concurrent(dispatchers: Dispatchers<ForFluxK> = FluxK.dispatchers()): Concurrent<ForFluxK> =
  object : FluxKConcurrent {
    override fun dispatchers(): Dispatchers<ForFluxK> = dispatchers
  }

@extension
interface FluxKDispatchers : Dispatchers<ForFluxK> {
  override fun default(): CoroutineContext =
    ComputationScheduler

  override fun io(): CoroutineContext =
    IOScheduler
}

@extension
interface FluxKEffect :
  Effect<ForFluxK>,
  FluxKAsync {
  override fun <A> FluxKOf<A>.runAsync(cb: (Either<Throwable, A>) -> FluxKOf<Unit>): FluxK<Unit> =
    fix().runAsync(cb)
}

@extension
interface FluxKConcurrentEffect :
  ConcurrentEffect<ForFluxK>,
  FluxKEffect {
  override fun <A> FluxKOf<A>.runAsyncCancellable(cb: (Either<Throwable, A>) -> FluxKOf<Unit>): FluxK<Disposable> =
    fix().runAsyncCancellable(cb)
}

fun FluxK.Companion.monadFlat(): FluxKMonad = monad()

fun FluxK.Companion.monadConcat(): FluxKMonad = object : FluxKMonad {
  override fun <A, B> FluxKOf<A>.flatMap(f: (A) -> FluxKOf<B>): FluxK<B> =
    fix().concatMap { f(it).fix() }
}

fun FluxK.Companion.monadSwitch(): FluxKMonad = object : FluxKMonadError {
  override fun <A, B> FluxKOf<A>.flatMap(f: (A) -> FluxKOf<B>): FluxK<B> =
    fix().switchMap { f(it).fix() }
}

fun FluxK.Companion.monadErrorFlat(): FluxKMonadError = monadError()

fun FluxK.Companion.monadErrorConcat(): FluxKMonadError = object : FluxKMonadError {
  override fun <A, B> FluxKOf<A>.flatMap(f: (A) -> FluxKOf<B>): FluxK<B> =
    fix().concatMap { f(it).fix() }
}

fun FluxK.Companion.monadErrorSwitch(): FluxKMonadError = object : FluxKMonadError {
  override fun <A, B> FluxKOf<A>.flatMap(f: (A) -> FluxKOf<B>): FluxK<B> =
    fix().switchMap { f(it).fix() }
}

fun <A> FluxK.Companion.fx(c: suspend ConcurrentSyntax<ForFluxK>.() -> A): FluxK<A> =
  FluxK.concurrent().fx.concurrent(c).fix()

@extension
interface FluxKTimer : Timer<ForFluxK> {
  override fun sleep(duration: Duration): FluxK<Unit> =
    FluxK(Mono.delay(java.time.Duration.ofNanos(duration.nanoseconds))
      .map { Unit }.toFlux())
}

@extension
interface FluxKFunctorFilter : FunctorFilter<ForFluxK>, FluxKFunctor {
  override fun <A, B> Kind<ForFluxK, A>.filterMap(f: (A) -> Option<B>): FluxK<B> =
    fix().filterMap(f)
}

@extension
interface FluxKMonadFilter : MonadFilter<ForFluxK>, FluxKMonad {
  override fun <A> empty(): FluxK<A> =
    Flux.empty<A>().k()

  override fun <A, B> Kind<ForFluxK, A>.filterMap(f: (A) -> Option<B>): FluxK<B> =
    fix().filterMap(f)
}
