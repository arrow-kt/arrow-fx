@file:Suppress("NOTHING_TO_INLINE", "INVISIBLE_REFERENCE", "INVISIBLE_MEMBER")
package arrow.fx.coroutines

import kotlin.coroutines.Continuation
import kotlin.coroutines.jvm.internal.probeCoroutineCreated as probe

internal inline fun <T> probeCoroutineCreated(completion: Continuation<T>): Continuation<T> =
  probe(completion)
