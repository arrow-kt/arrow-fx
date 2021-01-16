package arrow.fx.coroutines

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.retryWhen

fun <A, B> Flow<A>.retry(schedule: Schedule<Throwable, B>): Flow<A> = flow {
  (schedule as Schedule.ScheduleImpl<Any?, Throwable, B>)
  var dec: Schedule.Decision<Any?, B>
  var state: Any? = schedule.initialState()

  val retryWhen = retryWhen { cause, _ ->
    dec = schedule.update(cause, state)
    state = dec.state

    if (dec.cont) {
      delay(dec.delay.millis)
      true
    } else {
      false
    }
  }
  retryWhen.collect {
    emit(it)
  }
}
