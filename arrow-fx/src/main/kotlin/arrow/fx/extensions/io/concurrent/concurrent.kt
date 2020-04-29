package arrow.fx.extensions.io.concurrent

import arrow.Kind
import arrow.core.ListK
import arrow.core.extensions.listk.traverse.traverse
import arrow.core.fix
import arrow.core.identity
import arrow.core.k
import arrow.fx.IO
import arrow.fx.IOOf
import arrow.fx.IOPartialOf
import arrow.fx.MVar
import arrow.fx.Promise
import arrow.fx.Queue
import arrow.fx.Ref
import arrow.fx.Semaphore
import arrow.fx.extensions.IODefaultConcurrent
import arrow.fx.extensions.io.dispatchers.dispatchers
import arrow.fx.fix
import arrow.fx.typeclasses.Concurrent
import arrow.typeclasses.Applicative
import arrow.typeclasses.Traverse
import kotlin.coroutines.CoroutineContext

private val defaultConcurrent = object : IODefaultConcurrent {}

fun IO.Companion.concurrent(): Concurrent<IOPartialOf<Nothing>> =
  defaultConcurrent

fun <E> par(ctx: CoroutineContext) =
  object : Applicative<IOPartialOf<E>> {
    override fun <A> just(a: A): Kind<IOPartialOf<E>, A> = IO.just(a)

    override fun <A, B> Kind<IOPartialOf<E>, A>.ap(ff: Kind<IOPartialOf<E>, (A) -> B>): Kind<IOPartialOf<E>, B> =
      IO.parMapN(ctx, ff, this) { (f, a) -> f(a) }
  }

fun <G, E, A, B> Kind<G, A>.parTraverse(ctx: CoroutineContext, TG: Traverse<G>, f: (A) -> IOOf<E, B>): IO<E, Kind<G, B>> =
  TG.run { traverse(par(ctx), f) }.fix()

fun <E, A, B> Iterable<A>.parTraverse(ctx: CoroutineContext, f: (A) -> IOOf<E, B>): IO<E, List<B>> =
  toList().k().parTraverse(ctx, ListK.traverse(), f).map { it.fix() }

fun <E, A> Iterable<IOOf<E, A>>.parSequence(ctx: CoroutineContext): IO<E, List<A>> =
  parTraverse(ctx, ::identity)

fun <E, A> Iterable<IOOf<E, A>>.parSequence(): IO<E, List<A>> =
  parSequence(IO.dispatchers().default())

fun <E, A, B> Iterable<A>.parTraverse(f: (A) -> IOOf<E, B>): IO<E, List<B>> =
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
