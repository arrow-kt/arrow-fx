package arrow.fx.extensions.io.enviroment

import arrow.fx.IO
import arrow.fx.IOPartialOf
import arrow.fx.extensions.IOEnvironment
import arrow.fx.typeclasses.Environment

private val defaultEnviroment = object : IOEnvironment {}

fun IO.Companion.enviroment(): Environment<IOPartialOf<Nothing>> =
  defaultEnviroment
