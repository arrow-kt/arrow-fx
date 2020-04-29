package arrow.fx.extensions

import arrow.Kind
import arrow.core.Either
import arrow.core.Eval
import arrow.core.ListK
import arrow.core.Tuple2
import arrow.core.Tuple3
import arrow.core.extensions.listk.traverse.traverse
import arrow.core.fix
import arrow.core.identity
import arrow.core.k
import arrow.extension
import arrow.fx.IO
import arrow.fx.IOPartialOf
import arrow.fx.IOResult
import arrow.fx.IODispatchers
import arrow.fx.IOOf
import arrow.fx.MVar
import arrow.fx.OnCancel
import arrow.fx.Promise
import arrow.fx.Queue
import arrow.fx.Race2
import arrow.fx.Race3
import arrow.fx.RacePair
import arrow.fx.RaceTriple
import arrow.fx.Ref
import arrow.fx.Semaphore
import arrow.fx.Timer
import arrow.fx.extensions.io.dispatchers.dispatchers
import arrow.fx.extensions.io.concurrent.concurrent
import arrow.fx.extensions.io.concurrent.parTraverse
import arrow.fx.fix
import arrow.fx.typeclasses.Async
import arrow.fx.typeclasses.Bracket
import arrow.fx.typeclasses.CancelToken
import arrow.fx.typeclasses.Concurrent
import arrow.fx.typeclasses.ConcurrentSyntax
import arrow.fx.typeclasses.Dispatchers
import arrow.fx.typeclasses.Disposable
import arrow.fx.typeclasses.Environment
import arrow.fx.typeclasses.ExitCase
import arrow.fx.typeclasses.Fiber
import arrow.fx.typeclasses.MonadDefer
import arrow.fx.typeclasses.MonadIO
import arrow.fx.typeclasses.Proc
import arrow.fx.typeclasses.ProcF
import arrow.fx.typeclasses.UnsafeCancellableRun
import arrow.fx.typeclasses.UnsafeRun
import arrow.fx.typeclasses.toExitCase
import arrow.fx.unsafeRunAsync
import arrow.fx.unsafeRunAsyncCancellable
import arrow.fx.unsafeRunSync
import arrow.typeclasses.Applicative
import arrow.typeclasses.ApplicativeError
import arrow.typeclasses.Apply
import arrow.typeclasses.Continuation
import arrow.typeclasses.Functor
import arrow.typeclasses.Monad
import arrow.typeclasses.MonadError
import arrow.typeclasses.MonadThrow
import arrow.typeclasses.Monoid
import arrow.typeclasses.Semigroup
import arrow.typeclasses.SemigroupK
import arrow.typeclasses.stateStack
import arrow.typeclasses.suspended.BindSyntax
import arrow.unsafe
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlin.coroutines.resume
import kotlin.coroutines.startCoroutine
import arrow.fx.ap as Ap
import arrow.fx.flatMap as FlatMap
import arrow.fx.handleErrorWith as HandleErrorWith
import arrow.fx.redeemWith as RedeemWith
import arrow.fx.bracketCase as BracketCase
import arrow.fx.fork as Fork
import arrow.fx.guaranteeCase as GuaranteeCase

@extension
interface IOFunctor<E> : Functor<IOPartialOf<E>> {
  override fun <A, B> IOOf<E, A>.map(f: (A) -> B): IO<E, B> =
    fix().map(f)
}

@extension
interface IOApply<E> : Apply<IOPartialOf<E>> {
  override fun <A, B> IOOf<E, A>.ap(ff: IOOf<E, (A) -> B>): IO<E, B> =
    Ap(ff)

  override fun <A, B> IOOf<E, A>.apEval(ff: Eval<IOOf<E, (A) -> B>>): Eval<IO<E, B>> =
    Eval.now(Ap(IO.defer { ff.value() }))

  override fun <A, B> IOOf<E, A>.map(f: (A) -> B): IO<E, B> =
    fix().map(f)
}

@extension
interface IOApplicative<E> : Applicative<IOPartialOf<E>> {
  override fun <A> just(a: A): IO<E, A> =
    IO.just(a)

  override fun <A, B> IOOf<E, A>.ap(ff: IOOf<E, (A) -> B>): IO<E, B> =
    Ap(ff)

  override fun <A, B> IOOf<E, A>.apEval(ff: Eval<IOOf<E, (A) -> B>>): Eval<IO<E, B>> =
    Eval.now(Ap(IO.defer { ff.value() }))

  override fun <A, B> IOOf<E, A>.map(f: (A) -> B): IO<E, B> =
    fix().map(f)
}

@extension
interface IOMonad<E> : Monad<IOPartialOf<E>>, IOApplicative<E> {
  override fun <A, B> IOOf<E, A>.flatMap(f: (A) -> IOOf<E, B>): IO<E, B> =
    FlatMap(f)

  override fun <A, B> tailRecM(a: A, f: (A) -> IOOf<E, Either<A, B>>): IO<E, B> =
    IO.tailRecM(a, f)

  override fun <A, B> IOOf<E, A>.map(f: (A) -> B): IO<E, B> =
    fix().map(f)

  override fun <A, B> IOOf<E, A>.ap(ff: IOOf<E, (A) -> B>): IO<E, B> =
    Ap(ff)

  override fun <A, B> IOOf<E, A>.apEval(ff: Eval<IOOf<E, (A) -> B>>): Eval<IO<E, B>> =
    Eval.now(Ap(IO.defer { ff.value() }))
}

@extension
interface IOApplicativeError<E> : ApplicativeError<IOPartialOf<E>, Throwable>, IOApplicative<E> {
  override fun <A> IOOf<E, A>.attempt(): IO<E, Either<Throwable, A>> =
    fix().attempt()

  override fun <A> IOOf<E, A>.handleErrorWith(f: (Throwable) -> IOOf<E, A>): IO<E, A> =
    HandleErrorWith(f, { e -> IO.raiseError(e) })

  override fun <A> IOOf<E, A>.handleError(f: (Throwable) -> A): IO<E, A> =
    HandleErrorWith({ t -> IO.just(f(t)) }, { e -> IO.raiseError<E, A>(e) })

  override fun <A, B> IOOf<E, A>.redeem(fe: (Throwable) -> B, fb: (A) -> B): IO<E, B> =
    RedeemWith({ t -> IO.just(fe(t)) }, { e -> IO.raiseError<E, B>(e) }, { a -> IO.just(fb(a)) })

  override fun <A> raiseError(e: Throwable): IO<E, A> =
    IO.raiseException(e)

  override fun <A, B> IOOf<E, A>.apEval(ff: Eval<IOOf<E, (A) -> B>>): Eval<IO<E, B>> =
    Eval.now(Ap(IO.defer { ff.value() }))
}

@extension
interface IOMonadError<E> : MonadError<IOPartialOf<E>, Throwable>, IOApplicativeError<E>, IOMonad<E> {

  override fun <A> just(a: A): IO<Nothing, A> = IO.just(a)

  override fun <A, B> IOOf<E, A>.ap(ff: IOOf<E, (A) -> B>): IO<E, B> =
    Ap(ff)

  override fun <A, B> IOOf<E, A>.map(f: (A) -> B): IO<E, B> =
    fix().map(f)

  override fun <A> IOOf<E, A>.attempt(): IO<E, Either<Throwable, A>> =
    fix().attempt()

  override fun <A> IOOf<E, A>.handleErrorWith(f: (Throwable) -> IOOf<E, A>): IO<E, A> =
    HandleErrorWith(f, { e -> IO.raiseError(e) })

  override fun <A, B> IOOf<E, A>.redeemWith(fe: (Throwable) -> IOOf<E, B>, fb: (A) -> IOOf<E, B>): IO<E, B> =
    RedeemWith({ t -> fe(t) }, { e -> IO.raiseError(e) }, { a -> fb(a) })

  override fun <A> raiseError(e: Throwable): IO<Nothing, A> =
    IO.raiseException(e)

  override fun <A, B> IOOf<E, A>.apEval(ff: Eval<IOOf<E, (A) -> B>>): Eval<IO<E, B>> =
    Eval.now(Ap(IO.defer { ff.value() }))
}

@extension
interface IOMonadThrow<E> : MonadThrow<IOPartialOf<E>>, IOMonadError<E>

interface IOBracket : Bracket<IOPartialOf<Nothing>, Throwable>, IOMonadThrow<Nothing> {
  override fun <A, B> Kind<IOPartialOf<Nothing>, A>.bracketCase(release: (A, ExitCase<Throwable>) -> Kind<IOPartialOf<Nothing>, Unit>, use: (A) -> Kind<IOPartialOf<Nothing>, B>): Kind<IOPartialOf<Nothing>, B> =
    BracketCase(
      release = { a, exitCase -> release(a, exitCase.toExitCase()) },
      use = { a -> use(a) }
    )

  override fun <A> Kind<IOPartialOf<Nothing>, A>.guaranteeCase(finalizer: (ExitCase<Throwable>) -> Kind<IOPartialOf<Nothing>, Unit>): Kind<IOPartialOf<Nothing>, A> =
    GuaranteeCase { case -> finalizer(case.toExitCase()) }
}

interface IOMonadDefer : MonadDefer<IOPartialOf<Nothing>>, IOBracket {
  override fun <A> defer(fa: () -> IOOf<Nothing, A>): IO<Nothing, A> =
    IO.defer(fa)

  override fun lazy(): IO<Nothing, Unit> = IO.lazy
}

interface IOAsync : Async<IOPartialOf<Nothing>>, IOMonadDefer {
  override fun <A> async(fa: Proc<A>): IO<Nothing, A> =
    IO.async<Nothing, A> { cb ->
      fa { result ->
        when (result) {
          is Either.Left -> cb(IOResult.Exception(result.a))
          is Either.Right -> cb(IOResult.Success(result.b))
        }
      }
    }

  override fun <A> asyncF(k: ProcF<IOPartialOf<Nothing>, A>): IO<Nothing, A> =
    IO.asyncF<Nothing, A> { cb ->
      k { result ->
        when (result) {
          is Either.Left -> cb(IOResult.Exception(result.a))
          is Either.Right -> cb(IOResult.Success(result.b))
        }
      }
    }

  override fun <A> IOOf<Nothing, A>.continueOn(ctx: CoroutineContext): IO<Nothing, A> =
    fix().continueOn(ctx)

  override fun <A> effect(ctx: CoroutineContext, f: suspend () -> A): IO<Nothing, A> =
    IO.effect(ctx, f)

  override fun <A> effect(f: suspend () -> A): IO<Nothing, A> =
    IO.effect(f)
}

interface IOConcurrent : Concurrent<IOPartialOf<Nothing>>, IOAsync {
  override fun <A> Kind<IOPartialOf<Nothing>, A>.fork(ctx: CoroutineContext): IO<Nothing, Fiber<IOPartialOf<Nothing>, A>> =
    Fork(ctx)

  override fun <A> cancellable(k: ((Either<Throwable, A>) -> Unit) -> CancelToken<IOPartialOf<Nothing>>): IO<Nothing, A> =
    IO.cancellable { cb ->
      k { result ->
        when (result) {
          is Either.Left -> cb(IOResult.Exception(result.a))
          is Either.Right -> cb(IOResult.Success(result.b))
        }
      }
    }

  override fun <A> cancellableF(k: ((Either<Throwable, A>) -> Unit) -> IOOf<Nothing, CancelToken<IOPartialOf<Nothing>>>): IO<Nothing, A> =
    IO.cancellableF { cb ->
      k { result ->
        when (result) {
          is Either.Left -> cb(IOResult.Exception(result.a))
          is Either.Right -> cb(IOResult.Success(result.b))
        }
      }
    }

  override fun <A, B> CoroutineContext.racePair(fa: Kind<IOPartialOf<Nothing>, A>, fb: Kind<IOPartialOf<Nothing>, B>): IO<Nothing, RacePair<IOPartialOf<Nothing>, A, B>> =
    IO.racePair(this, fa, fb)

  override fun <A, B, C> CoroutineContext.raceTriple(fa: Kind<IOPartialOf<Nothing>, A>, fb: Kind<IOPartialOf<Nothing>, B>, fc: Kind<IOPartialOf<Nothing>, C>): IO<Nothing, RaceTriple<IOPartialOf<Nothing>, A, B, C>> =
    IO.raceTriple(this, fa, fb, fc)

  override fun <A, B> parTupledN(ctx: CoroutineContext, fa: Kind<IOPartialOf<Nothing>, A>, fb: Kind<IOPartialOf<Nothing>, B>): IO<Nothing, Tuple2<A, B>> =
    IO.parTupledN(ctx, fa, fb)

  override fun <A, B, C> parTupledN(ctx: CoroutineContext, fa: Kind<IOPartialOf<Nothing>, A>, fb: Kind<IOPartialOf<Nothing>, B>, fc: Kind<IOPartialOf<Nothing>, C>): IO<Nothing, Tuple3<A, B, C>> =
    IO.parTupledN(ctx, fa, fb, fc)

  override fun <A, B> CoroutineContext.raceN(fa: Kind<IOPartialOf<Nothing>, A>, fb: Kind<IOPartialOf<Nothing>, B>): IO<Nothing, Race2<A, B>> =
    IO.raceN(this@raceN, fa, fb)

  override fun <A, B, C> CoroutineContext.raceN(fa: Kind<IOPartialOf<Nothing>, A>, fb: Kind<IOPartialOf<Nothing>, B>, fc: Kind<IOPartialOf<Nothing>, C>): IO<Nothing, Race3<A, B, C>> =
    IO.raceN(this@raceN, fa, fb, fc)
}

fun IO.Companion.concurrent(dispatchers: Dispatchers<IOPartialOf<Nothing>>): Concurrent<IOPartialOf<Nothing>> = object : IOConcurrent {
  override fun dispatchers(): Dispatchers<IOPartialOf<Nothing>> = dispatchers
}

fun IO.Companion.timer(CF: Concurrent<IOPartialOf<Nothing>>): Timer<IOPartialOf<Nothing>> =
  Timer(CF)

@extension
interface IOSemigroup<E, A> : Semigroup<IO<E, A>> {

  fun SG(): Semigroup<A>

  override fun IO<E, A>.combine(b: IO<E, A>): IO<E, A> =
    FlatMap { a1: A -> b.map { a2: A -> SG().run { a1.combine(a2) } } }
}

@extension
interface IOMonoid<E, A> : Monoid<IO<E, A>>, IOSemigroup<E, A> {
  override fun SG(): Monoid<A>

  override fun empty(): IO<E, A> = IO.just(SG().empty())
}

interface IOUnsafeRun : UnsafeRun<IOPartialOf<Nothing>> {

  override suspend fun <A> unsafe.runBlocking(fa: () -> Kind<IOPartialOf<Nothing>, A>): A =
    fa().unsafeRunSync()

  override suspend fun <A> unsafe.runNonBlocking(fa: () -> Kind<IOPartialOf<Nothing>, A>, cb: (Either<Throwable, A>) -> Unit): Unit =
    fa().unsafeRunAsync(cb)
}

interface IOMonadIO : MonadIO<IOPartialOf<Nothing>>, IOMonad<Nothing> {
  override fun <A> IO<Nothing, A>.liftIO(): IO<Nothing, A> = this
}

private val MonadIO: MonadIO<IOPartialOf<Nothing>> =
  object : IOMonadIO {}

fun IO.Companion.monadIO(): MonadIO<IOPartialOf<Nothing>> =
  MonadIO

private val UnsafeRun: IOUnsafeRun =
  object : IOUnsafeRun {}

fun IO.Companion.unsafeRun(): UnsafeRun<IOPartialOf<Nothing>> =
  UnsafeRun

fun <A> unsafe.runBlocking(fa: () -> IOOf<Nothing, A>): A = invoke {
  UnsafeRun.run { runBlocking(fa) }
}

fun <A> unsafe.runNonBlocking(fa: () -> Kind<IOPartialOf<Nothing>, A>, cb: (Either<Throwable, A>) -> Unit): Unit = invoke {
  UnsafeRun.run { runNonBlocking(fa, cb) }
}

interface IOUnsafeCancellableRun : UnsafeCancellableRun<IOPartialOf<Nothing>>, IOUnsafeRun {
  override suspend fun <A> unsafe.runNonBlockingCancellable(onCancel: OnCancel, fa: () -> Kind<IOPartialOf<Nothing>, A>, cb: (Either<Throwable, A>) -> Unit): Disposable =
    fa().unsafeRunAsyncCancellable(onCancel, cb)
}

private val UnsafeCancellableRun: IOUnsafeCancellableRun =
  object : IOUnsafeCancellableRun {}

fun IO.Companion.unsafeCancellableRun(): UnsafeCancellableRun<IOPartialOf<Nothing>> =
  UnsafeCancellableRun

fun <A> unsafe.runNonBlockingCancellable(onCancel: OnCancel, fa: () -> Kind<IOPartialOf<Nothing>, A>, cb: (Either<Throwable, A>) -> Unit): Disposable =
  invoke {
    UnsafeCancellableRun.run {
      runNonBlockingCancellable(onCancel, fa, cb)
    }
  }

interface IODispatchers : Dispatchers<IOPartialOf<Nothing>> {
  override fun default(): CoroutineContext =
    IODispatchers.CommonPool

  override fun io(): CoroutineContext =
    IODispatchers.IOPool
}

interface IOEnvironment : Environment<IOPartialOf<Nothing>> {
  override fun dispatchers(): Dispatchers<IOPartialOf<Nothing>> =
    IO.dispatchers()

  override fun handleAsyncError(e: Throwable): IO<Nothing, Unit> =
    IO { println("Found uncaught async exception!"); e.printStackTrace() }
}

interface IODefaultConcurrent : Concurrent<IOPartialOf<Nothing>>, IOConcurrent {

  override fun dispatchers(): Dispatchers<IOPartialOf<Nothing>> =
    IO.dispatchers()
}

fun IO.Companion.timer(): Timer<IOPartialOf<Nothing>> = Timer(IO.concurrent())

fun <E, A> IO.Companion.fx(c: suspend IOSyntax<E>.() -> A): IO<E, A> =
  defer {
    val continuation = IOContinuation<E, A>()
    val wrapReturn: suspend IOContinuation<E, *>.() -> IO<E, A> = { just(c()) }
    wrapReturn.startCoroutine(continuation, continuation)
    continuation.returnedMonad().fix()
  }

@JvmName("fxIO")
fun <A> IO.Companion.fx(c: suspend ConcurrentSyntax<IOPartialOf<Nothing>>.() -> A): IO<Nothing, A> =
  IO.concurrent().fx.concurrent(c).fix()

/**
 * converts this Either to an IO. The resulting IO will evaluate to this Eithers
 * Right value or alternatively to the result of applying the specified function to this Left value.
 */
fun <A> Either<Throwable, A>.toIOException(): IO<Nothing, A> =
  fold({ IO.raiseException(it) }, { IO.just(it) })

/**
 * converts this Either to an IO. The resulting IO will evaluate to this Eithers
 * Right value or Left error value
 */
fun <E, A> Either<E, A>.toIO(): IO<E, A> =
  fold({ IO.raiseError(it) }, { IO.just(it) })

interface IOSyntax<E> : BindSyntax<IOPartialOf<E>> {
  suspend fun continueOn(ctx: CoroutineContext): Unit =
    IO.unit.continueOn(ctx).bind()

  fun <A> Iterable<IOOf<E, A>>.parSequence(ctx: CoroutineContext): IO<E, List<A>> =
    parTraverse(ctx, ::identity).fix()

  fun <A> Iterable<IOOf<E, A>>.parSequence(): IO<E, List<A>> =
    parSequence(IO.dispatchers().default())

  fun <A, B> Iterable<A>.parTraverse(ctx: CoroutineContext, f: (A) -> IOOf<E, B>): IO<E, List<B>> =
    toList().k().parTraverse(ctx, ListK.traverse(), f).map { it.fix() }

  fun <A, B> Iterable<A>.parTraverse(f: (A) -> IOOf<E, B>): IO<E, List<B>> =
    parTraverse(IO.dispatchers().default(), f)

  fun <A> Ref(a: A): IO<Nothing, Ref<IOPartialOf<Nothing>, A>> =
    Ref.invoke(IO.concurrent(), a).fix()

  fun <A> Promise(): IO<Nothing, Promise<IOPartialOf<Nothing>, A>> =
    Promise<IOPartialOf<Nothing>, A>(IO.concurrent()).fix()

  fun Semaphore(n: Long): IO<Nothing, Semaphore<IOPartialOf<Nothing>>> =
    Semaphore(n, IO.concurrent()).fix()

  fun <A> MVar(a: A): IO<Nothing, MVar<IOPartialOf<Nothing>, A>> =
    MVar(a, IO.concurrent()).fix()

  /**
   * Create an empty [MVar] or mutable variable structure to be used for thread-safe sharing.
   *
   * @see MVar
   * @see [MVar] for more usage details.
   */
  fun <A> MVar(): IO<Nothing, MVar<IOPartialOf<Nothing>, A>> =
    MVar.empty<IOPartialOf<Nothing>, A>(IO.concurrent()).fix()

  /** @see [Queue.Companion.bounded] **/
  fun <A> Queue.Companion.bounded(capacity: Int): IO<Nothing, Queue<IOPartialOf<Nothing>, A>> =
    Queue.bounded<IOPartialOf<Nothing>, A>(capacity, IO.concurrent()).fix()

  /** @see [Queue.Companion.sliding] **/
  fun <A> Queue.Companion.sliding(capacity: Int): IO<Nothing, Queue<IOPartialOf<Nothing>, A>> =
    Queue.sliding<IOPartialOf<Nothing>, A>(capacity, IO.concurrent()).fix()

  /** @see [Queue.Companion.dropping] **/
  fun <A> Queue.Companion.dropping(capacity: Int): IO<Nothing, Queue<IOPartialOf<Nothing>, A>> =
    Queue.dropping<IOPartialOf<Nothing>, A>(capacity, IO.concurrent()).fix()

  /** @see [Queue.Companion.unbounded] **/
  fun <A> Queue.Companion.unbounded(): IO<Nothing, Queue<IOPartialOf<Nothing>, A>> =
    Queue.unbounded<IOPartialOf<Nothing>, A>(IO.concurrent()).fix()
}

open class IOContinuation<E, A>(override val context: CoroutineContext = EmptyCoroutineContext) : Continuation<IO<E, A>>, IOSyntax<E> {

  override fun resume(value: IO<E, A>) {
    returnedMonad = value
  }

  @Suppress("UNCHECKED_CAST")
  override fun resumeWithException(exception: Throwable) {
    throw exception
  }

  protected lateinit var returnedMonad: IO<E, A>

  open fun returnedMonad(): IO<E, A> = returnedMonad

  override suspend fun <B> IOOf<E, B>.bind(): B =
    suspendCoroutineUninterceptedOrReturn { c ->
      val labelHere = c.stateStack // save the whole coroutine stack labels
      returnedMonad = this.FlatMap { x: B ->
        c.stateStack = labelHere
        c.resume(x)
        returnedMonad
      }
      COROUTINE_SUSPENDED
    }
}

@extension
interface IOSemigroupK<E> : SemigroupK<IOPartialOf<E>> {
  override fun <A> IOOf<E, A>.combineK(y: IOOf<E, A>): IO<E, A> =
    (this.fix() to y.fix()).let { (l, r) ->
      l.HandleErrorWith({ r }, { r })
    }
}
