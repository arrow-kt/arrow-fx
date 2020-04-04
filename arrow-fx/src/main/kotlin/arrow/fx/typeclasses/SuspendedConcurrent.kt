package arrow.fx.typeclasses

import arrow.Kind
import arrow.core.Tuple2
import arrow.core.toT
import arrow.fx.IO
import arrow.fx.IOPartialOf
import arrow.fx.RacePair
import arrow.fx.RaceTriple
import arrow.fx.extensions.IODefaultConcurrent
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.RestrictsSuspension
import kotlin.coroutines.startCoroutine

typealias Suspend<A> = suspend () -> A

// To include additional methods in the effect hierarchy without wrapping
interface SuspendedConcurrent<F> : Concurrent<F> {

  //TO be in async or concurrent
  fun <F, A> Suspend<A>.k(): Kind<F, A> = TODO()
  fun <A> Kind<*, A>.k(): Suspend<A> = TODO()

  //fork
  suspend fun <A> Suspend<A>.fork(ctx: CoroutineContext): Tuple2<Suspend<Unit>, Suspend<A>> {
    val k1: Kind<F, A> = k()
    val fiber = k1.fork(ctx).k()()
    return fiber.cancel().k() toT fiber.join().k()
  }

  suspend fun <A> eff(f: Suspend<A>): Suspend<A> = f

  suspend operator fun <A> Kind<F, A>.invoke(): A {
    return k()()
  }


  // these functions could return suspend instead but since they are already suspend they can provide a final value
  suspend fun <A, B> CoroutineContext.racePair(fa: Suspend<A>, fb: Suspend<B>): RacePair<F, A, B> {
    val k1: Kind<F, A> = fa.k()
    val k2: Kind<F, B> = fb.k()
    return racePair(k1, k2).k()()
  }

  suspend fun <A, B, C> CoroutineContext.raceTriple(fa: Suspend<A>, fb: Suspend<B>, fc: Suspend<C>): RaceTriple<F, A, B, C> {
    val k1: Kind<F, A> = fa.k()
    val k2: Kind<F, B> = fb.k()
    val k3: Kind<F, C> = fc.k()
    return raceTriple(k1, k2, k3).k()()
  }

  suspend fun continueOn(ctx: CoroutineContext): Unit =
    unit().continueOn(ctx).k()()

  suspend fun <A, B> Suspend<A>.bracketCase(
    release: suspend A.(ExitCase<Throwable>) -> Unit,
    use: suspend A.() -> B
  ): B {
    val k1: Kind<F, A> = k()
    val releaseK: (A, ExitCase<Throwable>) -> Kind<F, Unit> = { a, case -> effect { release(a, case) } }
    val useK: (A) -> Kind<F, B> = { a -> effect { use(a) } }
    return k1.bracketCase(releaseK, useK).k()()
  }

  override val fx: SuspendedConcurrentFx<F>
    get() = object : SuspendedConcurrentFx<F> {
      override val M: SuspendedConcurrent<F> = this@SuspendedConcurrent
    }

}

@Suppress("DELEGATED_MEMBER_HIDES_SUPERTYPE_OVERRIDE")
open class SuspendConcurrentContinuation<F, A>(private val CF: SuspendedConcurrent<F>, override val context: CoroutineContext = EmptyCoroutineContext) :
  AsyncContinuation<F, A>(CF), SuspendedConcurrent<F> by CF, SuspendedConcurrentSyntax<F> {
  override val fx: SuspendedConcurrentFx<F> = CF.fx
}

@RestrictsSuspension
interface SuspendedConcurrentSyntax<F> : SuspendedConcurrent<F>, ConcurrentSyntax<F>

interface SuspendedConcurrentFx<F> : ConcurrentFx<F> {
  override val M: SuspendedConcurrent<F>

  // Deferring in order to lazily launch the coroutine so it doesn't eagerly run on declaring context
  suspend operator fun <A> invoke(c: suspend SuspendedConcurrentSyntax<F>.() -> A): A = M.run {
    val continuation = SuspendConcurrentContinuation<F, A>(M)
    val wrapReturn: suspend SuspendedConcurrentSyntax<F>.() -> Kind<F, A> = { just(c()) }
    wrapReturn.startCoroutine(continuation, continuation)
    continuation.returnedMonad().k()()
  }
}

object IOSuspendedConcurrent : SuspendedConcurrent<IOPartialOf<Nothing>>, IODefaultConcurrent<Nothing>
object IOSuspendedFx : SuspendedConcurrentFx<IOPartialOf<Nothing>> {
  override val M: SuspendedConcurrent<IOPartialOf<Nothing>> = IOSuspendedConcurrent
}

suspend fun <A> IO.Companion.fx(f: suspend SuspendedConcurrentSyntax<IOPartialOf<Nothing>>.() -> A): A =
  IOSuspendedFx(f)
