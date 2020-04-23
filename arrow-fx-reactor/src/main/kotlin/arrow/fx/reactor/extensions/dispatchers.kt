package arrow.fx.reactor.extensions

import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext

internal val ComputationScheduler: CoroutineContext =
  Schedulers.parallel().asCoroutineContext()

internal val IOScheduler: CoroutineContext =
  Schedulers.elastic().asCoroutineContext()

fun Scheduler.asCoroutineContext(): CoroutineContext =
  SchedulerContext(this)

private class SchedulerContext(val scheduler: Scheduler) : AbstractCoroutineContextElement(ContinuationInterceptor), ContinuationInterceptor {
  override fun <T> interceptContinuation(continuation: Continuation<T>): Continuation<T> =
    SchedulerContinuation(scheduler, continuation.context.fold(continuation) { cont, element ->
      if (element != this@SchedulerContext && element is ContinuationInterceptor)
        element.interceptContinuation(cont) else cont
    })
}

private class SchedulerContinuation<T>(
  val scheduler: Scheduler,
  val cont: Continuation<T>
) : Continuation<T> {
  override val context: CoroutineContext = cont.context

  override fun resumeWith(result: Result<T>) {
    scheduler.schedule { cont.resumeWith(result) }
  }
}
