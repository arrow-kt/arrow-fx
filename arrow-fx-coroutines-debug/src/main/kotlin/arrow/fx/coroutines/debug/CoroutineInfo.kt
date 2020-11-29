@file:Suppress("UNUSED", "INVISIBLE_REFERENCE", "INVISIBLE_MEMBER")
package arrow.fx.coroutines.debug

import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext
import kotlin.coroutines.jvm.internal.CoroutineStackFrame

/**
 * Class describing coroutine info such as its context, state and stacktrace.
 */
class CoroutineInfo internal constructor(delegate: DebuggerInfo) {
  /**
   * [Coroutine context][coroutineContext] of the coroutine
   */
  val context: CoroutineContext =
    delegate.context

  /**
   * Last observed state of the coroutine
   */
  val state: State = State.valueOf(delegate.state)

//  private val creationStackBottom: CoroutineStackFrame? =
//    delegate.creationStackBottom

  val dispatcher: String? = delegate.dispatcher

  val lastObservedThreadState: String? = delegate.lastObservedThreadState

  val sequenceNumber: Long = delegate.sequenceNumber

  /**
   * Creation stacktrace of the coroutine.
   * Can be empty if [DebugProbes.enableCreationStackTraces] is not set.
   */
//  val creationStackTrace: List<StackTraceElement>
//    get() = creationStackTrace()

//  private val lastObservedFrame: CoroutineStackFrame? =
//    delegate.lastObservedFrame

  /**
   * Last observed stacktrace of the coroutine captured on its suspension or resumption point.
   * It means that for [running][State.RUNNING] coroutines resulting stacktrace is inaccurate and
   * reflects stacktrace of the resumption point, not the actual current stacktrace.
   */
//  public fun lastObservedStackTrace(): List<StackTraceElement> {
//    var frame: CoroutineStackFrame? = lastObservedFrame ?: return emptyList()
//    val result = ArrayList<StackTraceElement>()
//    while (frame != null) {
//      frame.getStackTraceElement()?.let { result.add(it) }
//      frame = frame.callerFrame
//    }
//    return result
//  }

//  private fun creationStackTrace(): List<StackTraceElement> {
//    val bottom = creationStackBottom ?: return emptyList()
//    // Skip "Coroutine creation stacktrace" frame
//    return sequence { yieldFrames(bottom.callerFrame) }.toList()
//  }

  private tailrec suspend fun SequenceScope<StackTraceElement>.yieldFrames(frame: CoroutineStackFrame?) {
    if (frame == null) return
    frame.getStackTraceElement()?.let { yield(it) }
    val caller = frame.callerFrame
    if (caller != null) {
      yieldFrames(caller)
    }
  }

  override fun toString(): String = "CoroutineInfo(state=$state,context=$context)"
}

/**
 * Current state of the coroutine.
 */
enum class State {
  /**
   * Created, but not yet started.
   */
  CREATED,
  /**
   * Started and running.
   */
  RUNNING,
  /**
   * Suspended.
   */
  SUSPENDED
}
