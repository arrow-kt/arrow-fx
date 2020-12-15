@file:Suppress("DEPRECATION_ERROR")
package arrow.fx.coroutines

import java.util.concurrent.TimeUnit

@Deprecated("Redundant since 0.12.0 will provide automatic integration with KotlinX", ReplaceWith("Duration", "kotlin.time.Duration"), DeprecationLevel.ERROR)
data class Duration(val amount: Long, val timeUnit: TimeUnit) {
  @Deprecated("Redundant property please use `inNanoseconds` in kotlin.time.Duration")
  val nanoseconds: Long by lazy { timeUnit.toNanos(amount) }
  @Deprecated("Redundant property please use `inNanoseconds` in kotlin.time.Duration")
  val millis: Long by lazy { timeUnit.toMillis(amount) }

  companion object {
    // Actually limited to 9223372036854775807 days, so unless you are very patient, it is unlimited ;-)
    val INFINITE = Duration(amount = Long.MAX_VALUE, timeUnit = TimeUnit.DAYS)
  }

  operator fun times(i: Int) = Duration(amount * i, timeUnit)

  operator fun plus(d: Duration): Duration = run {
    val comp = timeUnit.compareTo(d.timeUnit)
    when {
      comp == 0 -> Duration(amount + d.amount, timeUnit) // Same unit
      comp < 0 -> this + Duration(timeUnit.convert(d.amount, d.timeUnit), timeUnit) // Convert to same unit then add
      else -> d + this // Swap this and d to add to the smaller unit
    }
  }

  operator fun compareTo(d: Duration): Int = run {
    val comp = timeUnit.compareTo(d.timeUnit)
    when {
      comp == 0 -> amount.compareTo(d.amount)
      comp < 0 -> amount.compareTo(timeUnit.convert(d.amount, d.timeUnit))
      else -> -d.compareTo(this)
    }
  }
}

operator fun Int.times(d: Duration) = d * this

@Deprecated("Redundant since 0.12.0 will provide automatic integration with kotlin.time and KotlinX", ReplaceWith("days", "kotlin.time.days"), DeprecationLevel.ERROR)
val Long.days: Duration
  get() = Duration(this, TimeUnit.DAYS)

@Deprecated("Redundant since 0.12.0 will provide automatic integration with kotlin.time and KotlinX", ReplaceWith("days", "kotlin.time.days"), DeprecationLevel.ERROR)
val Int.days: Duration
  get() = Duration(this.toLong(), TimeUnit.DAYS)

@Deprecated("Redundant since 0.12.0 will provide automatic integration with kotlin.time and KotlinX", ReplaceWith("hours", "kotlin.time.hours"), DeprecationLevel.ERROR)
val Long.hours: Duration
  get() = Duration(this, TimeUnit.HOURS)

@Deprecated("Redundant since 0.12.0 will provide automatic integration with kotlin.time and KotlinX", ReplaceWith("hours", "kotlin.time.hours"), DeprecationLevel.ERROR)
val Int.hours: Duration
  get() = Duration(this.toLong(), TimeUnit.HOURS)

@Deprecated("Redundant since 0.12.0 will provide automatic integration with kotlin.time and KotlinX", ReplaceWith("microseconds", "kotlin.time.microseconds"), DeprecationLevel.ERROR)
val Long.microseconds: Duration
  get() = Duration(this, TimeUnit.MICROSECONDS)

@Deprecated("Redundant since 0.12.0 will provide automatic integration with kotlin.time and KotlinX", ReplaceWith("microseconds", "kotlin.time.microseconds"), DeprecationLevel.ERROR)
val Int.microseconds: Duration
  get() = Duration(this.toLong(), TimeUnit.MICROSECONDS)

@Deprecated("Redundant since 0.12.0 will provide automatic integration with kotlin.time and KotlinX", ReplaceWith("microseconds", "kotlin.time.microseconds"), DeprecationLevel.ERROR)
val Long.minutes: Duration
  get() = Duration(this, TimeUnit.MINUTES)

@Deprecated("Redundant since 0.12.0 will provide automatic integration with kotlin.time and KotlinX", ReplaceWith("minutes", "kotlin.time.minutes"), DeprecationLevel.ERROR)
val Int.minutes: Duration
  get() = Duration(this.toLong(), TimeUnit.MINUTES)

@Deprecated("Redundant since 0.12.0 will provide automatic integration with kotlin.time and KotlinX", ReplaceWith("milliseconds", "kotlin.time.milliseconds"), DeprecationLevel.ERROR)
val Long.milliseconds: Duration
  get() = Duration(this, TimeUnit.MILLISECONDS)

@Deprecated("Redundant since 0.12.0 will provide automatic integration with kotlin.time and KotlinX", ReplaceWith("milliseconds", "kotlin.time.milliseconds"), DeprecationLevel.ERROR)
val Int.milliseconds: Duration
  get() = Duration(this.toLong(), TimeUnit.MILLISECONDS)

@Deprecated("Redundant since 0.12.0 will provide automatic integration with kotlin.time and KotlinX", ReplaceWith("nanoseconds", "kotlin.time.nanoseconds"), DeprecationLevel.ERROR)
val Long.nanoseconds: Duration
  get() = Duration(this, TimeUnit.NANOSECONDS)

@Deprecated("Redundant since 0.12.0 will provide automatic integration with kotlin.time and KotlinX", ReplaceWith("nanoseconds", "kotlin.time.nanoseconds"), DeprecationLevel.ERROR)
val Int.nanoseconds: Duration
  get() = Duration(this.toLong(), TimeUnit.NANOSECONDS)

@Deprecated("Redundant since 0.12.0 will provide automatic integration with kotlin.time and KotlinX", ReplaceWith("seconds", "kotlin.time.seconds"), DeprecationLevel.ERROR)
val Long.seconds: Duration
  get() = Duration(this, TimeUnit.SECONDS)

@Deprecated("Redundant since 0.12.0 will provide automatic integration with kotlin.time and KotlinX", ReplaceWith("seconds", "kotlin.time.seconds"), DeprecationLevel.ERROR)
val Int.seconds: Duration
  get() = Duration(this.toLong(), TimeUnit.SECONDS)
