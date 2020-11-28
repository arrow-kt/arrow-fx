package arrow.fx.coroutines.debug

import net.bytebuddy.*
import net.bytebuddy.agent.*
import net.bytebuddy.dynamic.loading.*

/*
 * This class is used reflectively from arrow-fx-coroutines when this module is present in the classpath.
 * It is a substitute for service loading.
 */
internal class ByteBuddyDynamicAttach : Function1<Boolean, Unit> {
  override fun invoke(value: Boolean) {
    if (value) attach() else detach()
  }

  private fun attach() {
    ByteBuddyAgent.install(ByteBuddyAgent.AttachmentProvider.ForEmulatedAttachment.INSTANCE)
    val cl = Class.forName("kotlin.coroutines.jvm.internal.DebugProbesKt")
    val cl2 = Class.forName("arrow.fx.coroutines.debug.DebugProbesKt")

    ByteBuddy()
      .redefine(cl2)
      .name(cl.name)
      .make()
      .load(cl.classLoader, ClassReloadingStrategy.fromInstalledAgent())
  }

  private fun detach() {
    val cl = Class.forName("kotlin.coroutines.jvm.internal.DebugProbesKt")
    val cl2 = Class.forName("arrow.fx.coroutines.debug.NoOpProbesKt")
    ByteBuddy()
      .redefine(cl2)
      .name(cl.name)
      .make()
      .load(cl.classLoader, ClassReloadingStrategy.fromInstalledAgent())
  }
}
