package arrow.fx.coroutines

import arrow.fx.coroutines.debug.CoroutineName
import arrow.fx.coroutines.debug.DebugProbes
import org.junit.Test

class DevTest {

  @Test
  fun evalOn(): Unit = withDebugProbe {
    ForkConnected(CoroutineName("Outer fiber") + ComputationPool) {
      parTupledN(
        CoroutineName("parTupledN") + ComputationPool,
        { ForkConnected(CoroutineName("Inner fiber") + ComputationPool) { never<Unit>() } },
        { sleep(1000.milliseconds) },
        {
          while (true) {
            cancelBoundary()
          }
        }
      )
    }

    sleep(100.milliseconds)
    DebugProbes.dumpCoroutines()
  }

}
