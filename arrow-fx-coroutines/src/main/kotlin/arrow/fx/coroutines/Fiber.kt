package arrow.fx.coroutines

import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn

/**
 * [Fiber] represents a pure value that contains a running `suspend () -> A`.
 *
 * You can think of fibers as being lightweight threads. A [Fiber] is a
 * concurrency primitive for doing cooperative multi-tasking.
 */
interface Fiber<A> {

  suspend fun join(): A
  suspend fun cancel(): Unit

  companion object {

    /**
     * [Fiber] constructor.
     *
     * @param join task that will await for the completion of the underlying Fiber.
     * @param cancel task that will trigger the cancellation.
     */
    operator fun <A> invoke(join: suspend () -> A, cancel: CancelToken): Fiber<A> = object : Fiber<A> {
      override suspend fun join(): A = join()
      override suspend fun cancel(): Unit = cancel.invoke()
      override fun toString(): String = "Fiber(join= $join, cancel= $cancel)"
    }
  }
}

internal fun <A> Fiber(promise: UnsafePromise<A>, conn: SuspendConnection): Fiber<A> =
  Fiber({ promise.join() }, CancelToken { conn.cancel() })

/**
 * Launches a new suspendable cancellable coroutine within a [Fiber].
 * It does so by connecting the created [Fiber]'s cancellation to the callers `suspend` scope.
 * If the caller of `ForkConnected` gets cancelled, then this [Fiber] will also get cancelled.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.coroutines.*
 *
 * suspend fun main(): Unit {
 *   val parent = ForkConnected {
 *     ForkConnected { // cancellation connected to parent
 *        onCancel({ never<Unit>() }) {
 *          println("I got cancelled by my parent")
 *        }
 *     }
 *   }
 *   sleep(1.seconds)
 *   parent.cancel()
 * }
 * ```
 *
 * You can [Fiber.join] or [Fiber.cancel] the computation.
 * Cancelling this [Fiber] **will not** cancel its parent.
 */
suspend fun <A> ForkConnected(ctx: CoroutineContext, f: suspend () -> A): Fiber<A> =
  suspendCoroutineUninterceptedOrReturn { cont ->
    val conn = cont.context[SuspendConnection] ?: SuspendConnection.uncancellable

    val promise = UnsafePromise<A>()
    // A new SuspendConnection, because its cancellation is now decoupled from our current one.
    val conn2 = SuspendConnection()
    conn.push { conn2.cancel() }
    f.startCoroutineCancellable(CancellableContinuation(ctx, conn2, promise::complete))
    Fiber(promise, conn2)
  }

suspend fun <A> ForkConnected(f: suspend () -> A): Fiber<A> =
  ForkConnected(getDefaultContext(ComputationPool), f)

/** @see ForkConnected **/
suspend fun <A> (suspend () -> A).forkConnected(ctx: CoroutineContext): Fiber<A> =
  ForkConnected(ctx, this)

suspend fun <A> (suspend () -> A).forkConnected(): Fiber<A> =
  forkConnected(getDefaultContext(ComputationPool))

/**
 * Launches a new suspendable cancellable coroutine within a [Fiber].
 * It does so by connecting the created [Fiber]'s cancellation to the provided [interruptWhen].
 * If the [interruptWhen] signal gets triggered, then this [Fiber] will get cancelled.
 *
 * You can still cancel the [Fiber] independent from the [interruptWhen] token;
 * whichever one comes first cancels the [Fiber].
 *
 * This function is meant to integrate with 3rd party cancellation system such as Android.
 *
 * ```kotlin:ank:playground
 * import arrow.fx.coroutines.*
 *
 * tailrec suspend fun parallelProcess(): Unit {
 *   println(System.currentTimeMillis())
 *   sleep(1.seconds)
 *   parallelProcess()
 * }
 *
 * suspend fun main(): Unit {
 *   val switch = Promise<Unit>()
 *   val switcher = suspend {
 *     sleep(5.seconds)
 *     switch.complete(Unit)
 *   }
 *
 *   ::parallelProcess.forkScoped(interruptWhen = switch::get)
 *   switcher.forkConnected()
 * }
 * ```
 */
suspend fun <A> ForkScoped(
  ctx: CoroutineContext,
  interruptWhen: suspend () -> Unit,
  f: suspend () -> A
): Fiber<A> = suspendCoroutineUninterceptedOrReturn { cont ->
  val conn = cont.context[SuspendConnection] ?: SuspendConnection.uncancellable

  val promise = UnsafePromise<A>()
  // A new SuspendConnection, because its cancellation is now decoupled from our current one.
  val conn2 = SuspendConnection()
  conn.push{ conn2.cancel() }

  suspend { // Launch cancelation trigger system concurrently
    ForkConnected { interruptWhen.invoke(); conn2.cancel() }
    f.invoke() // Fire actual operation
  }.startCoroutineCancellable(CancellableContinuation(ctx, conn2, promise::complete))

  Fiber(promise, conn2)
}

suspend fun <A> ForkScoped(
  interruptWhen: suspend () -> Unit,
  f: suspend () -> A
): Fiber<A> = ForkScoped(getDefaultContext(ComputationPool), interruptWhen, f)

/** @see ForkScoped */
suspend fun <A> (suspend () -> A).forkScoped(
  ctx: CoroutineContext,
  interruptWhen: suspend () -> Unit
): Fiber<A> = ForkScoped(ctx, interruptWhen, this)

suspend fun <A> (suspend () -> A).forkScoped(interruptWhen: suspend () -> Unit): Fiber<A> =
  forkScoped(getDefaultContext(ComputationPool), interruptWhen)

/**
 * Launches a new suspendable cancellable coroutine within a [Fiber].
 * You can [Fiber.join] or [Fiber.cancel] the computation.
 *
 * **BEWARE**: you immediately leak the [Fiber] when launching without connection control.
 * Use [ForkConnected] or safely launch the fiber as a [Resource] or using [bracketCase].
 *
 * @see ForkConnected for a fork operation that wires cancellation to its parent in a safe way.
 */
suspend fun <A> ForkAndForget(ctx: CoroutineContext, f: suspend () -> A): Fiber<A> =
  f.forkAndForget(ctx)

suspend fun <A> ForkAndForget(f: suspend () -> A): Fiber<A> =
  ForkAndForget(getDefaultContext(ComputationPool), f)

/** @see ForkAndForget */
suspend fun <A> (suspend () -> A).forkAndForget(ctx: CoroutineContext): Fiber<A> {
  val promise = UnsafePromise<A>()
  // A new SuspendConnection, because its cancellation is now decoupled from our current one.
  val conn = SuspendConnection()
  startCoroutineCancellable(CancellableContinuation(ctx, conn, promise::complete))
  return Fiber(promise, conn)
}

suspend fun <A> (suspend () -> A).forkAndForget(): Fiber<A> = forkAndForget(getDefaultContext(ComputationPool))
