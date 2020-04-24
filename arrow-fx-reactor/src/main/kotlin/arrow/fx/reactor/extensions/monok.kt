package arrow.fx.reactor.extensions

import arrow.Kind
import arrow.core.Either
import arrow.core.Eval
import arrow.core.Tuple2
import arrow.core.Tuple3
import arrow.core.internal.AtomicBooleanW
import arrow.extension
import arrow.fx.RacePair
import arrow.fx.RaceTriple
import arrow.fx.Timer
import arrow.fx.reactor.CoroutineContextReactorScheduler.asScheduler
import arrow.fx.reactor.ForMonoK
import arrow.fx.reactor.MonoK
import arrow.fx.reactor.MonoKOf
import arrow.fx.reactor.extensions.monok.dispatchers.dispatchers
import arrow.fx.reactor.extensions.monok.async.async
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
import arrow.typeclasses.Functor
import arrow.typeclasses.Monad
import arrow.typeclasses.MonadError
import arrow.typeclasses.MonadThrow
import reactor.core.publisher.Mono
import reactor.core.publisher.MonoSink
import reactor.core.publisher.ReplayProcessor
import kotlin.coroutines.CoroutineContext
import arrow.fx.reactor.handleErrorWith as monoHandleErrorWith

@extension
interface MonoKFunctor : Functor<ForMonoK> {
  override fun <A, B> MonoKOf<A>.map(f: (A) -> B): MonoK<B> =
    fix().map(f)
}

@extension
interface MonoKApplicative : Applicative<ForMonoK>, MonoKFunctor {
  override fun <A, B> MonoKOf<A>.map(f: (A) -> B): MonoK<B> =
    fix().map(f)

  override fun <A, B> MonoKOf<A>.ap(ff: MonoKOf<(A) -> B>): MonoK<B> =
    fix().ap(ff)

  override fun <A> just(a: A): MonoK<A> =
    MonoK.just(a)

  override fun <A, B> Kind<ForMonoK, A>.apEval(ff: Eval<Kind<ForMonoK, (A) -> B>>): Eval<Kind<ForMonoK, B>> =
    Eval.now(fix().ap(MonoK.defer { ff.value() }))
}

@extension
interface MonoKMonad : Monad<ForMonoK>, MonoKApplicative {
  override fun <A, B> MonoKOf<A>.map(f: (A) -> B): MonoK<B> =
    fix().map(f)

  override fun <A, B> MonoKOf<A>.ap(ff: MonoKOf<(A) -> B>): MonoK<B> =
    fix().ap(ff)

  override fun <A, B> MonoKOf<A>.flatMap(f: (A) -> MonoKOf<B>): MonoK<B> =
    fix().flatMap(f)

  override fun <A, B> tailRecM(a: A, f: kotlin.Function1<A, MonoKOf<Either<A, B>>>): MonoK<B> =
    MonoK.tailRecM(a, f)

  override fun <A, B> Kind<ForMonoK, A>.apEval(ff: Eval<Kind<ForMonoK, (A) -> B>>): Eval<Kind<ForMonoK, B>> =
    Eval.now(fix().ap(MonoK.defer { ff.value() }))
}

@extension
interface MonoKApplicativeError : ApplicativeError<ForMonoK, Throwable>, MonoKApplicative {
  override fun <A> raiseError(e: Throwable): MonoK<A> =
    MonoK.raiseError(e)

  override fun <A> MonoKOf<A>.handleErrorWith(f: (Throwable) -> MonoKOf<A>): MonoK<A> =
    fix().monoHandleErrorWith { f(it).fix() }
}

@extension
interface MonoKMonadError : MonadError<ForMonoK, Throwable>, MonoKMonad, MonoKApplicativeError {
  override fun <A, B> MonoKOf<A>.map(f: (A) -> B): MonoK<B> =
    fix().map(f)

  override fun <A> raiseError(e: Throwable): MonoK<A> =
    MonoK.raiseError(e)

  override fun <A> MonoKOf<A>.handleErrorWith(f: (Throwable) -> MonoKOf<A>): MonoK<A> =
    fix().monoHandleErrorWith { f(it).fix() }
}

@extension
interface MonoKMonadThrow : MonadThrow<ForMonoK>, MonoKMonadError

@extension
interface MonoKBracket : Bracket<ForMonoK, Throwable>, MonoKMonadThrow {
  override fun <A, B> MonoKOf<A>.bracketCase(release: (A, ExitCase<Throwable>) -> MonoKOf<Unit>, use: (A) -> MonoKOf<B>): MonoK<B> =
    fix().bracketCase({ use(it) }, { a, e -> release(a, e) })

  override fun <A> Kind<ForMonoK, A>.guaranteeCase(finalizer: (ExitCase<Throwable>) -> Kind<ForMonoK, Unit>): MonoK<A> =
    fix().guaranteeCase(finalizer)
}

@extension
interface MonoKMonadDefer : MonadDefer<ForMonoK>, MonoKBracket {
  override fun <A> defer(fa: () -> MonoKOf<A>): MonoK<A> =
    MonoK.defer(fa)
}

@extension
interface MonoKAsync : Async<ForMonoK>, MonoKMonadDefer {
  override fun <A> async(fa: Proc<A>): MonoK<A> =
    MonoK.async(fa)

  override fun <A> asyncF(k: ProcF<ForMonoK, A>): MonoK<A> =
    MonoK.asyncF(k)

  override fun <A> MonoKOf<A>.continueOn(ctx: CoroutineContext): MonoK<A> =
    fix().continueOn(ctx)
}

interface MonoKConcurrent : Concurrent<ForMonoK>, MonoKAsync {

  override fun <A> Kind<ForMonoK, A>.fork(coroutineContext: CoroutineContext): MonoK<Fiber<ForMonoK, A>> =
    fix().fork(coroutineContext)

  override fun <A, B> parTupledN(ctx: CoroutineContext, fa: MonoKOf<A>, fb: MonoKOf<B>): MonoK<Tuple2<A, B>> =
    fa.value().zipWith(fb.value(), tupled()).subscribeOn(ctx.asScheduler()).k()

  override fun <A, B, C> parTupledN(ctx: CoroutineContext, fa: MonoKOf<A>, fb: MonoKOf<B>, fc: MonoKOf<C>): MonoK<Tuple3<A, B, C>> =
    Mono.zip(fa.value(), fb.value(), fc.value())
      .map { Tuple3(it.t1, it.t2, it.t3) }
      .subscribeOn(ctx.asScheduler()).k()

  override fun <A> cancellable(k: ((Either<Throwable, A>) -> Unit) -> CancelToken<ForMonoK>): MonoK<A> =
    MonoK.cancellable(k)

  override fun <A> cancellableF(k: ((Either<Throwable, A>) -> Unit) -> MonoKOf<CancelToken<ForMonoK>>): MonoK<A> =
    MonoK.cancellableF(k)

  override fun <A, B> CoroutineContext.racePair(fa: MonoKOf<A>, fb: MonoKOf<B>): MonoK<RacePair<ForMonoK, A, B>> =
    asScheduler().let { scheduler ->
      val active = AtomicBooleanW(true)

      Mono.create<RacePair<ForMonoK, A, B>> { emitter ->
        val sa = ReplayProcessor.create<A>()
        val sb = ReplayProcessor.create<B>()

        val dda = fa.value()
          .subscribeOn(scheduler)
          .subscribe(sa::onNext, sa::onError)
        val ddb = fb.value()
          .subscribeOn(scheduler)
          .subscribe(sb::onNext, sb::onError)

        emitter.onCancel { dda.dispose(); ddb.dispose() }

        val ffa = Fiber(sa.next().k(), MonoK { dda.dispose() })
        val ffb = Fiber(sb.next().k(), MonoK { ddb.dispose() })

        sa.subscribe({
          if (active.getAndSet(false)) emitter.success(RacePair.First(it, ffb))
        }, { e -> onError(active, ddb, emitter, e) }, { emitter.success() })

        sb.subscribe({
          if (active.getAndSet(false)) emitter.success(RacePair.Second(ffa, it))
        }, { e -> onError(active, dda, emitter, e) }, { emitter.success() })
      }
        .subscribeOn(scheduler)
        .k()
    }

  override fun <A, B, C> CoroutineContext.raceTriple(fa: MonoKOf<A>, fb: MonoKOf<B>, fc: MonoKOf<C>): MonoK<RaceTriple<ForMonoK, A, B, C>> =
    asScheduler().let { scheduler ->
      val active = AtomicBooleanW(true)

      Mono.create<RaceTriple<ForMonoK, A, B, C>> { emitter ->
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

        val ffa = Fiber(sa.next().k(), MonoK { dda.dispose() })
        val ffb = Fiber(sb.next().k(), MonoK { ddb.dispose() })
        val ffc = Fiber(sc.next().k(), MonoK { ddc.dispose() })

        sa.subscribe({
          if (active.getAndSet(false)) emitter.success(RaceTriple.First(it, ffb, ffc))
        }, { e -> onError(active, ddb, ddc, emitter, sa, e) }, { emitter.success() })

        sb.subscribe({
          if (active.getAndSet(false)) emitter.success(RaceTriple.Second(ffa, it, ffc))
        }, { e -> onError(active, dda, ddc, emitter, sb, e) }, { emitter.success() })

        sc.subscribe({
          if (active.getAndSet(false)) emitter.success(RaceTriple.Third(ffa, ffb, it))
        }, { e -> onError(active, dda, ddb, emitter, sc, e) }, { emitter.success() })
      }
        .subscribeOn(scheduler)
        .k()
    }

  private fun <A, B> onError(
    active: AtomicBooleanW,
    other2: reactor.core.Disposable,
    sink: MonoSink<RacePair<ForMonoK, A, B>>,
    err: Throwable
  ) = if (active.getAndSet(false)) {
    other2.dispose()
    sink.error(err)
  } else Unit

  private fun <A, B, C> onError(
    active: AtomicBooleanW,
    other2: reactor.core.Disposable,
    other3: reactor.core.Disposable,
    sink: MonoSink<RaceTriple<ForMonoK, A, B, C>>,
    processor: ReplayProcessor<*>,
    err: Throwable
  ): Unit = if (active.getAndSet(false)) {
    other2.dispose()
    other3.dispose()
    sink.error(err)
  } else Unit
}

fun MonoK.Companion.concurrent(dispatchers: Dispatchers<ForMonoK> = MonoK.dispatchers()): Concurrent<ForMonoK> =
  object : MonoKConcurrent {
    override fun dispatchers(): Dispatchers<ForMonoK> = dispatchers
  }

@extension
interface MonoKDispatchers : Dispatchers<ForMonoK> {
  override fun default(): CoroutineContext =
    ComputationScheduler

  override fun io(): CoroutineContext =
    IOScheduler
}

@extension
interface MonoKEffect : Effect<ForMonoK>, MonoKAsync {
  override fun <A> MonoKOf<A>.runAsync(cb: (Either<Throwable, A>) -> MonoKOf<Unit>): MonoK<Unit> =
    fix().runAsync(cb)
}

@extension
interface MonoKConcurrentEffect : ConcurrentEffect<ForMonoK>, MonoKEffect {
  override fun <A> MonoKOf<A>.runAsyncCancellable(cb: (Either<Throwable, A>) -> MonoKOf<Unit>): MonoK<Disposable> =
    fix().runAsyncCancellable(cb)
}

@extension
interface MonoKTimer : Timer<ForMonoK> {
  override fun sleep(duration: Duration): MonoK<Unit> =
    MonoK(Mono.delay(java.time.Duration.ofNanos(duration.nanoseconds))
      .map { Unit })
}

fun <A> MonoK.Companion.fx(c: suspend ConcurrentSyntax<ForMonoK>.() -> A): MonoK<A> =
  MonoK.concurrent().fx.concurrent(c).fix()
