package arrow.fx.coroutines.debug

import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext

/**
 * A [CoroutineContext] that wraps a `name`,
 * it also participates in the debugging facilities.
 */
class CoroutineName(val name: String) : AbstractCoroutineContextElement(CoroutineName) {
  companion object Key : CoroutineContext.Key<CoroutineName>
}

/**
 * When running with [DEBUG] enabled this is used to keep track of the number of Coroutines running.
 */
// If we support ThreadContextElement, then we can also use this to update thread names that are running this coroutine.
// See KotlinX: https://github.com/Kotlin/kotlinx.coroutines/blob/master/kotlinx-coroutines-core/jvm/src/CoroutineContext.kt#L60
internal class CoroutineId(val id: Long) : AbstractCoroutineContextElement(CoroutineId) {
  companion object Key : CoroutineContext.Key<CoroutineId>
}

/**
 * Name of the property that controls coroutine debugging.
 *
 * ### Debugging facilities
 *
 * In debug mode every coroutine is assigned a unique consecutive identifier.
 *
 * Enable debugging facilities with `arrow.fx.coroutines.debug` ([DEBUG_PROPERTY_NAME]) system property,
 * use the following values:
 *
 * * "`auto`" (default mode, [DEBUG_PROPERTY_VALUE_AUTO]) -- enabled when assertions are enabled with "`-ea`" JVM option.
 * * "`on`" ([DEBUG_PROPERTY_VALUE_ON]) or empty string -- enabled.
 * * "`off`" ([DEBUG_PROPERTY_VALUE_OFF]) -- disabled.
 *
 * Coroutine name can be explicitly assigned using [CoroutineName] context element.
 * The string "coroutine" is used as a default name.
 *
 * Debugging facilities are implemented by [newCoroutineContext][CoroutineScope.newCoroutineContext] function that
 * is used in all coroutine builders to create context of a new coroutine.
 */
const val DEBUG_PROPERTY_NAME: String = "arrow.fx.coroutines.debug"

/**
 * Automatic debug configuration value for [DEBUG_PROPERTY_NAME].
 */
const val DEBUG_PROPERTY_VALUE_AUTO: String = "auto"

/**
 * Debug turned on value for [DEBUG_PROPERTY_NAME].
 */
const val DEBUG_PROPERTY_VALUE_ON: String = "on"

/**
 * Debug turned on value for [DEBUG_PROPERTY_NAME].
 */
const val DEBUG_PROPERTY_VALUE_OFF: String = "off"

internal val ASSERTIONS_ENABLED: Boolean =
  CoroutineName::class.java.desiredAssertionStatus()

internal val DEBUG: Boolean = systemProp(DEBUG_PROPERTY_NAME).let { value ->
  when (value) {
    DEBUG_PROPERTY_VALUE_AUTO, null -> ASSERTIONS_ENABLED
    DEBUG_PROPERTY_VALUE_ON, "" -> true
    DEBUG_PROPERTY_VALUE_OFF -> false
    else -> error("System property '$DEBUG_PROPERTY_NAME' has unrecognized value '$value'")
  }
}


// It is used only in debug mode
internal val COROUTINE_ID: AtomicLong =
  AtomicLong(0)

// for tests only
internal fun resetCoroutineId() {
  COROUTINE_ID.set(0)
}

internal const val DEFAULT_COROUTINE_NAME = "coroutine"

internal fun systemProp(propertyName: String): String? =
  try {
    System.getProperty(propertyName)
  } catch (e: SecurityException) {
    null
  }

/**
 * Creates context for the new coroutine. It installs [Dispatchers.Default] when no other dispatcher nor
 * [ContinuationInterceptor] is specified, and adds optional support for debugging facilities (when turned on).
 *
 * See [DEBUG_PROPERTY_NAME] for description of debugging facilities on JVM.
 */
fun newCoroutineContext(context: CoroutineContext): CoroutineContext =
  if (DEBUG) context + CoroutineId(COROUTINE_ID.incrementAndGet()) else context

internal fun CoroutineContext.coroutineName(): String? {
  if (!DEBUG) return null
  val coroutineId = this[CoroutineId] ?: return null
  val coroutineName = this[CoroutineName]?.name ?: DEFAULT_COROUTINE_NAME
  return "$coroutineName#${coroutineId.id}"
}

@PublishedApi
internal fun <A> Continuation<A>.toDebugString(default: String): String {
  val coroutineName = context.coroutineName() ?: return default
  return "$coroutineName:$default"
}
