package arrow.fx.extensions.io.monadDefer

import arrow.fx.IO
import arrow.fx.IOPartialOf
import arrow.fx.extensions.IOMonadDefer
import arrow.fx.typeclasses.MonadDefer

private val defaultMonadDefer = object : IOMonadDefer { }

fun IO.Companion.monadDefer(): MonadDefer<IOPartialOf<Nothing>> =
  defaultMonadDefer
