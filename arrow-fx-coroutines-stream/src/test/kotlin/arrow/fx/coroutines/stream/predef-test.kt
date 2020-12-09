package arrow.fx.coroutines.stream

import arrow.core.Either
import arrow.core.identity
import arrow.fx.coroutines.ComputationPool
import io.kotest.property.arbitrary.constant
import io.kotest.assertions.fail
import io.kotest.property.Arb
import io.kotest.property.arbitrary.bind
import io.kotest.property.arbitrary.char
import io.kotest.property.arbitrary.choice
import io.kotest.property.arbitrary.map
import io.kotest.property.arbitrary.string
import kotlin.coroutines.Continuation
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlin.coroutines.intrinsics.intercepted
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.startCoroutine

fun Arb.Companion.throwable(): Arb<Throwable> =
  Arb.string().map(::RuntimeException)

fun <A> Arb.Companion.result(right: Arb<A>): Arb<Result<A>> {
  val failure: Arb<Result<A>> = Arb.throwable().map { e -> Result.failure<A>(e) }
  val success: Arb<Result<A>> = right.map { a -> Result.success(a) }
  return Arb.choice(failure, success)
}

fun Arb.Companion.charRange(): Arb<CharRange> =
  Arb.bind(Arb.char(), Arb.char()) { a, b ->
    if (a < b) a..b else b..a
  }

fun Arb.Companion.unit(): Arb<Unit> =
  Arb.constant(Unit)

/** Useful for testing success & error scenarios with an `Either` generator **/
internal fun <A> Either<Throwable, A>.rethrow(): A =
  fold({ throw it }, ::identity)

internal fun <A> Result<A>.toEither(): Either<Throwable, A> =
  fold({ a -> Either.Right(a) }, { e -> Either.Left(e) })

internal suspend fun Throwable.suspend(): Nothing =
  suspendCoroutineUninterceptedOrReturn { cont ->
    suspend { throw this }.startCoroutine(Continuation(ComputationPool) {
      cont.intercepted().resumeWith(it)
    })

    COROUTINE_SUSPENDED
  }

internal suspend fun <A> A.suspend(): A =
  suspendCoroutineUninterceptedOrReturn { cont ->
    suspend { this }.startCoroutine(Continuation(ComputationPool) {
      cont.intercepted().resumeWith(it)
    })

    COROUTINE_SUSPENDED
  }

internal fun <A> A.suspended(): suspend () -> A =
  suspend { suspend() }

internal suspend fun <A> Either<Throwable, A>.suspend(): A =
  suspendCoroutineUninterceptedOrReturn { cont ->
    suspend { this }.startCoroutine(Continuation(ComputationPool) {
      it.fold(
        {
          it.fold(
            { e -> cont.intercepted().resumeWithException(e) },
            { a -> cont.intercepted().resume(a) }
          )
        },
        { e -> cont.intercepted().resumeWithException(e) }
      )
    })

    COROUTINE_SUSPENDED
  }

internal fun <A> Either<Throwable, A>.suspended(): suspend () -> A =
  suspend { suspend() }

/**
 * Example usage:
 * ```kotlin
 * val exception = assertThrows<IllegalArgumentException> {
 *     throw IllegalArgumentException("Talk to a duck")
 * }
 * assertEquals("Talk to a duck", exception.message)
 * ```
 * @see Assertions.assertThrows
 */
inline fun <A> assertThrowable(executable: () -> A): Throwable {
  val a = try {
    executable.invoke()
  } catch (e: Throwable) {
    e
  }

  return if (a is Throwable) a else fail("Expected an exception but found: $a")
}

internal suspend fun CoroutineContext.shift(): Unit =
  suspendCoroutineUninterceptedOrReturn { cont ->
    suspend { this }.startCoroutine(Continuation(this) {
      cont.resume(Unit)
    })

    COROUTINE_SUSPENDED
  }
