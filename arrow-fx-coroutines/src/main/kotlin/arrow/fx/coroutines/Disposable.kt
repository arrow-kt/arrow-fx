package arrow.fx.coroutines

typealias Disposable = () -> Unit

internal fun SuspendConnection.toDisposable(): Disposable = {
  Platform.unsafeRunSync { cancel() }
}
