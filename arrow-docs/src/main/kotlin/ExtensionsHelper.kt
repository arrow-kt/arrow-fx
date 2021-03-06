package arrow.reflect

import kotlin.reflect.KClass

private val lineSeparator: String = System.getProperty("line.separator")

private val moduleNames: Map<String, String> = mapOf(
  "arrow.aql" to "arrow-aql",
  "arrow.aql.extensions" to "arrow-aql",
  "arrow.core.extensions" to "arrow.core",
  "arrow.core.internal" to "arrow.core",
  "arrow.core" to "arrow-core-data",
  "arrow.typeclasses" to "arrow-core-data",
  "arrow.typeclasses.internal" to "arrow-core-data",
  "arrow.typeclasses.suspended" to "arrow-core-data",
  "arrow.free.extensions" to "arrow-free",
  "arrow.free" to "arrow-free-data",
  "arrow.fx" to "arrow-fx",
  "arrow.fx.extensions" to "arrow-fx",
  "arrow.fx.internal" to "arrow-fx",
  "arrow.fx.typeclasses" to "arrow-fx",
  "arrow.fx.mtl" to "arrow-fx-mtl",
  "arrow.fx.reactor" to "arrow-fx-reactor",
  "arrow.fx.reactor.extensions" to "arrow-fx-reactor",
  "arrow.fx.rx2" to "arrow-fx-rx2",
  "arrow.fx.rx2.extensions" to "arrow-fx-rx2",
  "arrow.mtl.extensions" to "arrow-mtl",
  "arrow.mtl" to "arrow-mtl-data",
  "arrow.mtl.typeclasses" to "arrow-mtl-data",
  "arrow.optics" to "arrow-optics",
  "arrow.optics.dsl" to "arrow-optics",
  "arrow.optics.extensions" to "arrow-optics",
  "arrow.optics.std" to "arrow-optics",
  "arrow.optics.typeclasses" to "arrow-optics",
  "arrow.optics.mtl" to "arrow-optics-mtl",
  "arrow.recursion.extensions" to "arrow-recursion",
  "arrow.recursion.data" to "arrow-recursion-data",
  "arrow.recursion.pattern" to "arrow-recursion-data",
  "arrow.recursion.typeclasses" to "arrow-recursion-data",
  "arrow.reflect" to "arrow-reflect",
  "arrow.ui.extensions" to "arrow-ui",
  "arrow.ui" to "arrow-ui-data",
  "arrow.validation.refinedTypes" to "arrow-validation",
  "arrow.validation.refinedTypes.bool" to "arrow-validation",
  "arrow.validation.refinedTypes.generic" to "arrow-validation",
  "arrow.validation.refinedTypes.numeric" to "arrow-validation"
)

fun String.toKebabCase(): String {
  var text: String = ""
  this.forEach {
    if (it.isUpperCase()) {
      text += "-"
      text += it.toLowerCase()
    } else {
      text += it
    }
  }
  return text
}

fun <A : Any> KClass<A>.docsMarkdownLink(moduleName: String?, packageName: String): String =
  "[$simpleName]({{ '/apidocs/$moduleName/${packageName.toKebabCase()}/${simpleName?.toKebabCase()}' | relative_url }})"
