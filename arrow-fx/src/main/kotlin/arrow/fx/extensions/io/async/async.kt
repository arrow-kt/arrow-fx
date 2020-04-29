package arrow.fx.extensions.io.async

import arrow.fx.IO
import arrow.fx.IOPartialOf
import arrow.fx.extensions.IOAsync
import arrow.fx.typeclasses.Async

private val defaultAsync = object : IOAsync {}

fun IO.Companion.async(): Async<IOPartialOf<Nothing>> =
  defaultAsync
