package arrow.fx.coroutines

import kotlin.coroutines.Continuation
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.startCoroutine

/**
 * Environment that can run [suspend] programs using [startCoroutine] and [startCoroutineCancellable].
 *
 * An [Environment] runs on a certain [CoroutineContext] which is used to start the programs on.
 * Since coroutines always return where they were started, this [CoroutineContext] also defines where you return after operators like [sleep] or [evalOn].
 * Therefore it's advised to always run on [ComputationPool] which is the default setting.
 *
 * [Environment] also has an [asyncErrorHandler] which by default redirects to [Throwable.printStackTrace],
 * no user flow errors will every be send here [CancelToken] exceptions might bubble up here when they cannot be redirect to the user.
 *
 * This [Environment] is meant to be used in Java frameworks, or frameworks that do not expose [suspend] edge-points.
 *
 * This contract could be elaborated on Android to provide automatic cancellation on Android LifecycleOwner.
 */
interface Environment {

  /**
   * Start [CoroutineContext] of the programs ran using [startCoroutine] and [startCoroutineCancellable].
   */
  val ctx: CoroutineContext

  /**
   * The async error handler is reserved for rare exceptions.
   * When an error occurs after the completion of a [kotlin.coroutines.Continuation],
   * or when an error occurs when triggering a [CancelToken].
   *
   * This will never be invoked for a [Throwable] that occurred in a user flow.
   */
  fun asyncErrorHandler(e: Throwable): Unit

  /**
   * Execution strategy that will block the current thread that's waiting for the program to yield a value.
   */
  fun <A> unsafeRunSync(fa: suspend () -> A): A

  /**
   * Execution strategies that will immediately return and perform the program's work without blocking the current thread.
   * This operation runs uncancellable.
   */
  fun unsafeRunAsync(fa: suspend () -> Unit): Unit =
    unsafeRunAsync(fa, { throw it }, { /* Finished normally */ })

  /**
   * Runs and forgets the operation.
   * This operation runs uncancellable.
   */
  fun <A> unsafeRunAsync(fa: suspend () -> A, e: (Throwable) -> Unit, a: (A) -> Unit): Unit

  /**
   * Runs the operation in a cancelable way
   * Operation can be cancelled by invoking the returend [Disposable]
   */
  fun unsafeRunAsyncCancellable(fa: suspend () -> Unit): Disposable =
    unsafeRunAsyncCancellable(fa, { throw it }, { /* Finished normally */ })

  fun <A> unsafeRunAsyncCancellable(fa: suspend () -> A, e: (Throwable) -> Unit, a: (A) -> Unit): Disposable

  companion object {
    operator fun invoke(ctx: CoroutineContext = ComputationPool): Environment =
      DefaultEnvironment(ctx)
  }
}

internal class DefaultEnvironment(override val ctx: CoroutineContext) : Environment {

  override fun asyncErrorHandler(e: Throwable) =
    e.printStackTrace()

  override fun <A> unsafeRunSync(fa: suspend () -> A): A =
    Platform.unsafeRunSync(ctx, fa)

  override fun <A> unsafeRunAsync(fa: suspend () -> A, e: (Throwable) -> Unit, a: (A) -> Unit): Unit =
    fa.startCoroutine(Continuation(ctx) { res -> res.fold(a, e) })

  override fun <A> unsafeRunAsyncCancellable(fa: suspend () -> A, e: (Throwable) -> Unit, a: (A) -> Unit): Disposable =
    fa.startCoroutineCancellable(CancellableContinuation(ctx) { res ->
      res.fold(a, e) // Return error to caller
    })
}
