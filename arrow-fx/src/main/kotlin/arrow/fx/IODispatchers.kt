package arrow.fx

import arrow.fx.internal.AtomicIntW
import arrow.undocumented
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ForkJoinPool
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext

@undocumented
// FIXME use expected and actual for multiplatform
@Deprecated(IODeprecation)
object IODispatchers {
  // FIXME use ForkJoinPool.commonPool() in Java 8
  val CommonPool: CoroutineContext = ForkJoinPool().asCoroutineContext()

  private val ioCtr = AtomicIntW(0)
  val IOPool = Executors.newCachedThreadPool { r ->
    Thread(r).apply {
      name = "io-arrow-kt-worker-${ioCtr.getAndIncrement()}"
      isDaemon = true
    }
  }.asCoroutineContext()
}

@Deprecated(IODeprecation, ReplaceWith("Resource.fromExecutor { this }", "arrow.fx.coroutines"))
fun ExecutorService.asCoroutineContext(): CoroutineContext =
  ExecutorServiceContext(this)

private class ExecutorServiceContext(val pool: ExecutorService) : AbstractCoroutineContextElement(ContinuationInterceptor), ContinuationInterceptor {
  override fun <T> interceptContinuation(continuation: Continuation<T>): Continuation<T> =
    ExecutorServiceContinuation(pool, continuation.context.fold(continuation) { cont, element ->
      if (element != this@ExecutorServiceContext && element is ContinuationInterceptor)
        element.interceptContinuation(cont) else cont
    })
}

private class ExecutorServiceContinuation<T>(
  val pool: ExecutorService,
  val cont: Continuation<T>
) : Continuation<T> {
  override val context: CoroutineContext = cont.context

  override fun resumeWith(result: Result<T>) {
    pool.execute { cont.resumeWith(result) }
  }
}
