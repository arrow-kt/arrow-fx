package arrow.fx

import arrow.core.test.UnitSpec
import io.kotest.core.spec.Spec
import io.reactivex.plugins.RxJavaPlugins

abstract class RxJavaSpec : UnitSpec() {

  override fun beforeSpec(spec: Spec) {
    super.beforeSpec(spec)
    RxJavaPlugins.setErrorHandler { }
  }

  override fun afterSpec(spec: Spec) {
    super.afterSpec(spec)
    RxJavaPlugins.setErrorHandler(null)
  }
}
