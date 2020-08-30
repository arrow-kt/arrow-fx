package arrow.fx.coroutines

import arrow.core.Either
import arrow.core.Tuple4
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.bool
import io.kotest.property.arbitrary.element
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.string
import java.util.concurrent.Executors

class ParTupledNTest : ArrowFxSpec(spec = {

  "parTupledN 2 returns to original context" {
    val mapCtxName = "parTupled2"
    val mapCtx = Resource.fromExecutor { Executors.newFixedThreadPool(2, NamedThreadFactory { mapCtxName }) }

    checkAll {
      single.zip(mapCtx).use { (single, mapCtx) ->
        evalOn(single) {
          threadName() shouldBe singleThreadName

          val (s1, s2) = parTupledN(mapCtx, { threadName() }, { threadName() })

          s1 shouldBe mapCtxName
          s2 shouldBe mapCtxName
          threadName() shouldBe singleThreadName
        }
      }
    }
  }

  "parTupledN 2 returns to original context on failure" {
    val mapCtxName = "parTupled2"
    val mapCtx = Resource.fromExecutor { Executors.newFixedThreadPool(2, NamedThreadFactory { mapCtxName }) }

    checkAll(Arb.int(1..2), Arb.throwable()) { choose, e ->
      single.zip(mapCtx).use { (single, mapCtx) ->
        evalOn(single) {
          threadName() shouldBe singleThreadName

          Either.catch {
            when (choose) {
              1 -> parTupledN(mapCtx, { e.suspend() }, { never<Nothing>() })
              else -> parTupledN(mapCtx, { never<Nothing>() }, { e.suspend() })
            }
          } shouldBe Either.Left(e)

          threadName() shouldBe singleThreadName
        }
      }
    }
  }

  "ParTupledN 2 runs in parallel" {
    checkAll(Arb.int(), Arb.int()) { a, b ->
      val r = Atomic("")
      val modifyGate = Promise<Int>()

      parTupledN(
        {
          modifyGate.get()
          r.update { i -> "$i$a" }
        },
        {
          r.set("$b")
          modifyGate.complete(0)
        }
      )

      r.get() shouldBe "$b$a"
    }
  }

  "ParTupledN 2 finishes on single thread" {
    checkAll(Arb.string()) { name ->
      single.use { ctx ->
        parTupledN(ctx, threadName, threadName)
      } shouldBe Pair("single", "single")
    }
  }

  "Cancelling ParTupledN 2 cancels all participants" {
    checkAll(Arb.int(), Arb.int()) { a, b ->
      val s = Semaphore(0L)
      val pa = Promise<Pair<Int, ExitCase>>()
      val pb = Promise<Pair<Int, ExitCase>>()

      val loserA = suspend { guaranteeCase({ s.release(); never<Int>() }) { ex -> pa.complete(Pair(a, ex)) } }
      val loserB = suspend { guaranteeCase({ s.release(); never<Int>() }) { ex -> pb.complete(Pair(b, ex)) } }

      val f = ForkAndForget { parTupledN(loserA, loserB) }

      s.acquireN(2) // Suspend until all racers started
      f.cancel()

      pa.get() shouldBe Pair(a, ExitCase.Cancelled)
      pb.get() shouldBe Pair(b, ExitCase.Cancelled)
    }
  }

  "ParTupledN 2 cancels losers if a failure occurs in one of the tasks" {
    checkAll(
      Arb.throwable(),
      Arb.bool(),
      Arb.int()
    ) { e, leftWinner, a ->
      val s = Semaphore(0L)
      val pa = Promise<Pair<Int, ExitCase>>()

      val winner = suspend { s.acquire(); throw e }
      val loserA = suspend { guaranteeCase({ s.release(); never<Int>() }) { ex -> pa.complete(Pair(a, ex)) } }

      val r = Either.catch {
        if (leftWinner) parTupledN(winner, loserA)
        else parTupledN(loserA, winner)
      }

      pa.get() shouldBe Pair(a, ExitCase.Cancelled)
      r shouldBe Either.Left(e)
    }
  }

  "parTupledN 3 returns to original context" {
    val mapCtxName = "parTupled3"
    val mapCtx = Resource.fromExecutor { Executors.newFixedThreadPool(3, NamedThreadFactory { mapCtxName }) }

    checkAll {
      single.zip(mapCtx).use { (single, mapCtx) ->
        evalOn(single) {
          threadName() shouldBe singleThreadName

          val (s1, s2, s3) = parTupledN(mapCtx, { threadName() }, { threadName() }, { threadName() })

          s1 shouldBe mapCtxName
          s2 shouldBe mapCtxName
          s3 shouldBe mapCtxName
          threadName() shouldBe singleThreadName
        }
      }
    }
  }

  "parTupledN 3 returns to original context on failure" {
    val mapCtxName = "parTupled3"
    val mapCtx = Resource.fromExecutor { Executors.newFixedThreadPool(3, NamedThreadFactory { mapCtxName }) }

    checkAll(Arb.int(1..3), Arb.throwable()) { choose, e ->
      single.zip(mapCtx).use { (single, mapCtx) ->
        evalOn(single) {
          threadName() shouldBe singleThreadName

          Either.catch {
            when (choose) {
              1 -> parTupledN(mapCtx, { e.suspend() }, { never<Nothing>() }, { never<Nothing>() })
              2 -> parTupledN(mapCtx, { never<Nothing>() }, { e.suspend() }, { never<Nothing>() })
              else -> parTupledN(mapCtx, { never<Nothing>() }, { never<Nothing>() }, { e.suspend() })
            }
          } shouldBe Either.Left(e)

          threadName() shouldBe singleThreadName
        }
      }
    }
  }

  "ParTupledN 3 runs in parallel" {
    checkAll(Arb.int(), Arb.int(), Arb.int()) { a, b, c ->
      val r = Atomic("")
      val modifyGate1 = Promise<Unit>()
      val modifyGate2 = Promise<Unit>()

      parTupledN(
        {
          modifyGate2.get()
          r.update { i -> "$i$a" }
        },
        {
          modifyGate1.get()
          r.update { i -> "$i$b" }
          modifyGate2.complete(Unit)
        },
        {
          r.set("$c")
          modifyGate1.complete(Unit)
        }
      )

      r.get() shouldBe "$c$b$a"
    }
  }

  "ParTupledN 3 finishes on single thread" {
    single.use { ctx ->
      parTupledN(ctx, threadName, threadName, threadName)
    } shouldBe Triple("single", "single", "single")
  }

  "Cancelling ParTupledN 3 cancels all participants" {
    checkAll(Arb.int(), Arb.int(), Arb.int()) { a, b, c ->
      val s = Semaphore(0L)
      val pa = Promise<Pair<Int, ExitCase>>()
      val pb = Promise<Pair<Int, ExitCase>>()
      val pc = Promise<Pair<Int, ExitCase>>()

      val loserA = suspend { guaranteeCase({ s.release(); never<Int>() }) { ex -> pa.complete(Pair(a, ex)) } }
      val loserB = suspend { guaranteeCase({ s.release(); never<Int>() }) { ex -> pb.complete(Pair(b, ex)) } }
      val loserC = suspend { guaranteeCase({ s.release(); never<Int>() }) { ex -> pc.complete(Pair(c, ex)) } }

      val f = ForkAndForget { parTupledN(loserA, loserB, loserC) }

      s.acquireN(3) // Suspend until all racers started
      f.cancel()

      pa.get() shouldBe Pair(a, ExitCase.Cancelled)
      pb.get() shouldBe Pair(b, ExitCase.Cancelled)
      pc.get() shouldBe Pair(c, ExitCase.Cancelled)
    }
  }

  "ParTupledN 3 cancels losers if a failure occurs in one of the tasks" {
    checkAll(
      Arb.throwable(),
      Arb.element(listOf(1, 2, 3)),
      Arb.int(),
      Arb.int()
    ) { e, leftWinner, a, b ->
      val s = Semaphore(0L)
      val pa = Promise<Pair<Int, ExitCase>>()
      val pb = Promise<Pair<Int, ExitCase>>()

      val winner = suspend { s.acquireN(2); throw e }
      val loserA = suspend { guaranteeCase({ s.release(); never<Int>() }) { ex -> pa.complete(Pair(a, ex)) } }
      val loserB = suspend { guaranteeCase({ s.release(); never<Int>() }) { ex -> pb.complete(Pair(b, ex)) } }

      val res = Either.catch {
        when (leftWinner) {
          1 -> raceN(winner, loserA, loserB)
          2 -> raceN(loserA, winner, loserB)
          else -> raceN(loserA, loserB, winner)
        }
      }

      pa.get() shouldBe Pair(a, ExitCase.Cancelled)
      pb.get() shouldBe Pair(b, ExitCase.Cancelled)
      res shouldBe Either.Left(e)
    }
  }

  "parTupledN 4 returns to original context" {
    val mapCtxName = "parTupled4"
    val mapCtx = Resource.fromExecutor { Executors.newFixedThreadPool(4, NamedThreadFactory { mapCtxName }) }

    checkAll {
      single.zip(mapCtx).use { (single, mapCtx) ->
        evalOn(single) {
          threadName() shouldBe singleThreadName

          val (s1, s2, s3, s4) = parTupledN(mapCtx, { threadName() }, { threadName() }, { threadName() }, { threadName() })

          s1 shouldBe mapCtxName
          s2 shouldBe mapCtxName
          s3 shouldBe mapCtxName
          s4 shouldBe mapCtxName
          threadName() shouldBe singleThreadName
        }
      }
    }
  }

  "parTupledN 4 returns to original context on failure" {
    val mapCtxName = "parTupled3"
    val mapCtx = Resource.fromExecutor { Executors.newFixedThreadPool(4, NamedThreadFactory { mapCtxName }) }

    checkAll(Arb.int(1..4), Arb.throwable()) { choose, e ->
      single.zip(mapCtx).use { (single, mapCtx) ->
        evalOn(single) {
          threadName() shouldBe singleThreadName

          Either.catch {
            when (choose) {
              1 -> parTupledN(mapCtx, { e.suspend() }, { never<Nothing>() }, { never<Nothing>() }, { never<Nothing>() })
              2 -> parTupledN(mapCtx, { never<Nothing>() }, { e.suspend() }, { never<Nothing>() }, { never<Nothing>() })
              3 -> parTupledN(mapCtx, { never<Nothing>() }, { never<Nothing>() }, { e.suspend() }, { never<Nothing>() })
              else -> parTupledN(mapCtx, { never<Nothing>() }, { never<Nothing>() }, { never<Nothing>() }, { e.suspend() })
            }
          } shouldBe Either.Left(e)

          threadName() shouldBe singleThreadName
        }
      }
    }
  }

  "ParTupledN 4 runs in parallel" {
    checkAll(Arb.int(), Arb.int(), Arb.int(), Arb.int()) { a, b, c, d ->
      val r = Atomic("")
      val modifyGate1 = Promise<Unit>()
      val modifyGate2 = Promise<Unit>()
      val modifyGate3 = Promise<Unit>()

      parTupledN(
        {
          modifyGate3.get()
          r.update { i -> "$i$a" }
        },
        {
          modifyGate2.get()
          r.update { i -> "$i$b" }
          modifyGate3.complete(Unit)
        },
        {
          modifyGate1.get()
          r.update { i -> "$i$c" }
          modifyGate2.complete(Unit)
        },
        {
          r.set("$d")
          modifyGate1.complete(Unit)
        }
      )

      r.get() shouldBe "$d$c$b$a"
    }
  }

  "ParTupledN 4 finishes on single thread" {
    single.use { ctx ->
      parTupledN(ctx, threadName, threadName, threadName, threadName)
    } shouldBe Tuple4("single", "single", "single", "single")
  }

  "Cancelling ParTupledN 4 cancels all participants" {
    checkAll(Arb.int(), Arb.int(), Arb.int(), Arb.int()) { a, b, c, d ->
      val s = Semaphore(0L)
      val pa = Promise<Pair<Int, ExitCase>>()
      val pb = Promise<Pair<Int, ExitCase>>()
      val pc = Promise<Pair<Int, ExitCase>>()
      val pd = Promise<Pair<Int, ExitCase>>()

      val loserA = suspend { guaranteeCase({ s.release(); never<Int>() }) { ex -> pa.complete(Pair(a, ex)) } }
      val loserB = suspend { guaranteeCase({ s.release(); never<Int>() }) { ex -> pb.complete(Pair(b, ex)) } }
      val loserC = suspend { guaranteeCase({ s.release(); never<Int>() }) { ex -> pc.complete(Pair(c, ex)) } }
      val loserD = suspend { guaranteeCase({ s.release(); never<Int>() }) { ex -> pd.complete(Pair(d, ex)) } }

      val f = ForkAndForget { parTupledN(loserA, loserB, loserC, loserD) }

      s.acquireN(4) // Suspend until all racers started
      f.cancel()

      pa.get() shouldBe Pair(a, ExitCase.Cancelled)
      pb.get() shouldBe Pair(b, ExitCase.Cancelled)
      pc.get() shouldBe Pair(c, ExitCase.Cancelled)
      pd.get() shouldBe Pair(d, ExitCase.Cancelled)
    }
  }

  "ParTupledN 4 cancels losers if a failure occurs in one of the tasks" {
    checkAll(
      Arb.throwable(),
      Arb.element(listOf(1, 2, 3, 4)),
      Arb.int(),
      Arb.int(),
      Arb.int()
    ) { e, leftWinner, a, b, c ->
      val s = Semaphore(0L)
      val pa = Promise<Pair<Int, ExitCase>>()
      val pb = Promise<Pair<Int, ExitCase>>()
      val pc = Promise<Pair<Int, ExitCase>>()

      val winner = suspend { s.acquireN(3); throw e }
      val loserA = suspend { guaranteeCase({ s.release(); never<Int>() }) { ex -> pa.complete(Pair(a, ex)) } }
      val loserB = suspend { guaranteeCase({ s.release(); never<Int>() }) { ex -> pb.complete(Pair(b, ex)) } }
      val loserC = suspend { guaranteeCase({ s.release(); never<Int>() }) { ex -> pc.complete(Pair(c, ex)) } }

      val res = Either.catch {
        when (leftWinner) {
          1 -> raceN(winner, loserA, loserB, loserC)
          2 -> raceN(loserA, winner, loserB, loserC)
          3 -> raceN(loserA, loserB, winner, loserC)
          4 -> raceN(loserA, loserB, loserC, winner)
        }
      }

      pa.get() shouldBe Pair(a, ExitCase.Cancelled)
      pb.get() shouldBe Pair(b, ExitCase.Cancelled)
      pc.get() shouldBe Pair(c, ExitCase.Cancelled)
      res shouldBe Either.Left(e)
    }
  }
})
