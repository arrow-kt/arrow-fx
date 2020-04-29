package arrow.fx.extensions.io.bracket

import arrow.fx.IO
import arrow.fx.IOPartialOf
import arrow.fx.extensions.IOBracket
import arrow.fx.typeclasses.Bracket

private val defaultBracket = object : IOBracket {}

fun IO.Companion.bracket(): Bracket<IOPartialOf<Nothing>, Throwable> =
  defaultBracket
