package arrow.test.generators

import arrow.Kind
import arrow.fx.ForIO
import arrow.fx.IO
import io.kotlintest.properties.Gen
import java.util.concurrent.TimeUnit

fun Gen.Companion.timeUnit(): Gen<TimeUnit> = Gen.from(TimeUnit.values())

fun IO.Companion.genK() = object : GenK<ForIO> {
  override fun <A> genK(gen: Gen<A>): Gen<Kind<ForIO, A>> = Gen.oneOf(
    gen.map(IO.Companion::just),
    Gen.throwable().map(IO.Companion::raiseError)
  )
}
