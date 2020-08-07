package arrow.fx.coroutines

import kotlinx.atomicfu.atomic
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ForkJoinPool
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext
val ComputationPool: CoroutineContext = ForkJoinPool().asCoroutineContext()

private object IOCounter {
  private val ref = atomic(0)
  fun getAndIncrement(): Int = ref.getAndIncrement()
}

val IOPool = Executors.newCachedThreadPool { r ->
  Thread(r).apply {
    name = "io-arrow-kt-worker-${IOCounter.getAndIncrement()}"
    isDaemon = true
  }
}.asCoroutineContext()

internal fun ExecutorService.asCoroutineContext(): CoroutineContext =
  ExecutorServiceContext(this)

private class ExecutorServiceContext(val pool: ExecutorService) :
  AbstractCoroutineContextElement(ContinuationInterceptor), ContinuationInterceptor {
  override fun <T> interceptContinuation(continuation: Continuation<T>): Continuation<T> =
    ExecutorServiceContinuation(pool, continuation.context.fold(continuation) { cont, element ->
      if (element != this@ExecutorServiceContext && element is ContinuationInterceptor)
        element.interceptContinuation(cont) else cont
    })
}

private class ExecutorServiceContinuation<T>(val pool: ExecutorService, val cont: Continuation<T>) : Continuation<T> {
  override val context: CoroutineContext = cont.context

  override fun resumeWith(result: Result<T>) {
    pool.execute { cont.resumeWith(result) }
  }
}
