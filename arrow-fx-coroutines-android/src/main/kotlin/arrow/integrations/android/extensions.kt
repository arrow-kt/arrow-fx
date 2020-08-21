package arrow.integrations.android

import androidx.lifecycle.Lifecycle.Event
import androidx.lifecycle.Lifecycle.State
import androidx.lifecycle.LifecycleEventObserver
import androidx.lifecycle.LifecycleOwner
import arrow.core.Either
import arrow.fx.coroutines.CancelToken
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.cancellable
import arrow.fx.coroutines.stream.fix

/**
 * Hooks a Stream with the given [LifecycleOwner] to be interrupted on the specified [Event].
 */
fun <A> Stream<A>.interruptWhen(
  owner: LifecycleOwner,
  event: Event
) {
  interruptWhen(owner.asStream().map { it == event })
}

fun LifecycleOwner.asStream(): Stream<Event> =
  Stream.cancellable {
    val observer = LifecycleEventObserver { _, event -> emit(event) }
    lifecycle.addObserver(observer)
    CancelToken { lifecycle.removeObserver(observer) }
  }
