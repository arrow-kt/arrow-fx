package arrow.fx.coroutines.kotlinx

import arrow.fx.coroutines.CancellableContinuation
import arrow.fx.coroutines.Disposable
import arrow.fx.coroutines.Environment
import arrow.fx.coroutines.startCoroutineCancellable
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlin.coroutines.Continuation
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.startCoroutine

interface KotlinXEnvironment : Environment, CoroutineScope {

  val scope: CoroutineScope

  val handler: CoroutineExceptionHandler
    get() = CoroutineExceptionHandler { _, e -> asyncErrorHandler(e) }

  override val coroutineContext: CoroutineContext
    get() = ctx

  override val ctx: CoroutineContext
    get() = scope.coroutineContext

  fun <A> unsafeRunSyncScoped(fa: suspend CoroutineScope.() -> A): A

  fun <A> unsafeRunAsyncScoped(fa: suspend CoroutineScope.() -> A, e: (Throwable) -> Unit, a: (A) -> Unit)

  fun <A> unsafeRunAsyncCancellableScoped(fa: suspend CoroutineScope.() -> A, e: (Throwable) -> Unit, a: (A) -> Unit): Disposable

  override fun <A> unsafeRunSync(fa: suspend () -> A): A =
    unsafeRunSyncScoped { fa() }

  override fun <A> unsafeRunAsync(fa: suspend () -> A, e: (Throwable) -> Unit, a: (A) -> Unit) =
    unsafeRunAsyncScoped({ fa() }, e, a)

  override fun <A> unsafeRunAsyncCancellable(fa: suspend () -> A, e: (Throwable) -> Unit, a: (A) -> Unit): Disposable =
    unsafeRunAsyncCancellableScoped({ fa() }, e, a)
}

operator fun Environment.Companion.invoke(
  scope: CoroutineScope,
  asyncErrorHandler: (Throwable) -> Unit = { it.printStackTrace() }
): KotlinXEnvironment = object : KotlinXEnvironment {

  override val scope: CoroutineScope = scope + handler

  override fun asyncErrorHandler(e: Throwable) =
    asyncErrorHandler(e)

  override fun <R, A> unsafeRunSync(receiver: R, fa: suspend R.() -> A): A =
    runBlocking(ctx) { fa.invoke(receiver) }

  override fun <A> unsafeRunSyncScoped(fa: suspend CoroutineScope.() -> A): A =
    runBlocking(ctx, fa)

  override fun <A> unsafeRunAsyncScoped(fa: suspend CoroutineScope.() -> A, e: (Throwable) -> Unit, a: (A) -> Unit) =
    fa.startCoroutine(scope, Continuation(ctx) { res -> res.fold(a, e) })

  override fun <R, A> unsafeRunAsync(receiver: R, fa: suspend R.() -> A, e: (Throwable) -> Unit, a: (A) -> Unit) =
    fa.startCoroutine(receiver, Continuation(ctx) { res -> res.fold(a, e) })

  override fun <R, A> unsafeRunAsyncCancellable(receiver: R, fa: suspend R.() -> A, e: (Throwable) -> Unit, a: (A) -> Unit): Disposable =
    if (scope.isActive) {
      val disposable = fa.startCoroutineCancellable(receiver, CancellableContinuation(ctx) { res ->
        res.fold(a, e)
      })

      scope.launch {
        suspendCancellableCoroutine { cont ->
          cont.invokeOnCancellation { disposable.invoke() }
        }
      }

      disposable
    } else {
      { Unit }
    }

  override fun <A> unsafeRunAsyncCancellableScoped(fa: suspend CoroutineScope.() -> A, e: (Throwable) -> Unit, a: (A) -> Unit): Disposable =
    unsafeRunAsyncCancellable(scope, fa, e, a)

  fun launch(
    context: CoroutineContext = EmptyCoroutineContext,
    start: CoroutineStart = CoroutineStart.DEFAULT,
    block: suspend CoroutineScope.() -> Unit
  ): Job =
    scope.launch(context, start) {
      suspendCancellable {
        block.invoke(this)
      }
    }

  fun <T> async(
    context: CoroutineContext = EmptyCoroutineContext,
    start: CoroutineStart = CoroutineStart.DEFAULT,
    block: suspend CoroutineScope.() -> T
  ): Deferred<T> =
    scope.async(context, start) {
      suspendCancellable {
        block.invoke(this)
      }
    }
}
