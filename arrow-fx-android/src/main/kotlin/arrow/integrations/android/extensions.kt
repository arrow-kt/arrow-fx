package arrow.integrations.android

import androidx.lifecycle.Lifecycle.Event
import androidx.lifecycle.Lifecycle.State
import androidx.lifecycle.LifecycleEventObserver
import androidx.lifecycle.LifecycleOwner
import arrow.fx.IO
import arrow.fx.IOOf
import arrow.fx.IOResult
import arrow.fx.extensions.io.async.shift
import arrow.fx.extensions.io.monad.followedBy
import arrow.fx.fix
import arrow.fx.flatMap
import kotlin.coroutines.CoroutineContext

/**
 * Unsafely run an [IO] and receive the values in a callback [cb] while participating in structured concurrency.
 * Equivalent of [IO.unsafeRunAsyncCancellableEither] but with its cancellation token wired to [LifecycleOwner].
 *
 * @see [IO.unsafeRunAsyncCancellableEither] for a version that returns the cancellation token instead.
 */
fun <E, A> LifecycleOwner.unsafeRunIO(io: IOOf<E, A>, cb: (IOResult<E, A>) -> Unit): Unit =
  io.unsafeRunScoped(this, cb)

/**
 * Unsafely run an [IO] and receive the values in a callback [cb] while participating in structured concurrency.
 * Equivalent of [IO.unsafeRunAsyncCancellableEither] but with its cancellation token wired to [LifecycleOwner].
 *
 * @see [IO.unsafeRunAsyncCancellableEither] for a version that returns the cancellation token instead.
 */
fun <E, A> IOOf<E, A>.unsafeRunScoped(
  owner: LifecycleOwner,
  cb: (IOResult<E, A>) -> Unit
) {
  if (owner.lifecycle.currentState.isAtLeast(State.CREATED)) {
    val disposable = fix().unsafeRunAsyncCancellableEither(cb = cb)

    owner.lifecycle.addObserver(object : LifecycleEventObserver {
      override fun onStateChanged(source: LifecycleOwner, event: Event) {
        if (event == Event.ON_DESTROY) {
          disposable()
          source.lifecycle.removeObserver(this)
        }
      }
    })
  }
}

fun <E, A> IOOf<E, A>.deferUntilActive(
  ctx: CoroutineContext,
  owner: LifecycleOwner
): IO<E, A> {
  fun ioOfEvent(eventTarget: Event): IO<Nothing, Unit> = ctx.shift<Nothing>().followedBy(IO.async { cb ->
    owner.lifecycle.addObserver(object : LifecycleEventObserver {
      override fun onStateChanged(source: LifecycleOwner, event: Event) {
        if (event == eventTarget) {
          source.lifecycle.removeObserver(this)
          cb(IOResult.Success(Unit))
        }
      }
    })
  })

  return when {
    owner.lifecycle.currentState.isAtLeast(State.CREATED) -> {
      IO.racePair(
        ctx, this, ioOfEvent(Event.ON_DESTROY)
      ).flatMap {
        it.fold(ifA = { a, _ ->
          IO.just(a)
        }, ifB = { fiber, _ ->
          ioOfEvent(Event.ON_CREATE).flatMap { fiber.join() }
        })
      }
    }
    else -> {
      ioOfEvent(Event.ON_CREATE).flatMap { this }
    }
  }
}
