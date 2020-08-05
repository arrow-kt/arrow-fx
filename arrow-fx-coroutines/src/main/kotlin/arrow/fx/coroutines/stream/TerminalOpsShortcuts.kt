package arrow.fx.coroutines.stream

/**
 * This is a shortcut for [compile] + [TerminalOps.forEach]
 */
suspend fun <O> Stream<O>.forEach(block: (O) -> Unit): Unit =
  compile().forEach(block)
