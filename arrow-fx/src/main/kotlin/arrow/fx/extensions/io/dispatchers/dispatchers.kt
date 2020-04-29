package arrow.fx.extensions.io.dispatchers

import arrow.fx.IO
import arrow.fx.IOPartialOf
import arrow.fx.extensions.IODispatchers
import arrow.fx.typeclasses.Dispatchers

private val defaultDispatchers = object : IODispatchers {}

fun IO.Companion.dispatchers(): Dispatchers<IOPartialOf<Nothing>> =
  defaultDispatchers
