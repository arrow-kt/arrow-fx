package arrow.fx.coroutines.debug

import kotlin.coroutines.Continuation

/*
 * Empty class used to replace installed agent in the end of debug session
 */
@JvmName("probeCoroutineResumed")
internal fun probeCoroutineResumedNoOp(frame: Continuation<*>) = Unit

@JvmName("probeCoroutineSuspended")
internal fun probeCoroutineSuspendedNoOp(frame: Continuation<*>) = Unit

@JvmName("probeCoroutineCreated")
internal fun <T> probeCoroutineCreatedNoOp(completion: Continuation<T>): Continuation<T> = completion
