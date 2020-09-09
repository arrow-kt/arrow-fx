package arrow.fx.coroutines.parMapN

import arrow.core.Either
import arrow.core.Tuple4
import arrow.fx.coroutines.ArrowFxSpec
import arrow.fx.coroutines.Atomic
import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.ForkAndForget
import arrow.fx.coroutines.NamedThreadFactory
import arrow.fx.coroutines.Promise
import arrow.fx.coroutines.Resource
import arrow.fx.coroutines.Semaphore
import arrow.fx.coroutines.evalOn
import arrow.fx.coroutines.guaranteeCase
import arrow.fx.coroutines.never
import arrow.fx.coroutines.parMapN
import arrow.fx.coroutines.single
import arrow.fx.coroutines.singleThreadName
import arrow.fx.coroutines.suspend
import arrow.fx.coroutines.threadName
import arrow.fx.coroutines.throwable
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.element
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.string
import java.util.concurrent.Executors

class ParMap4Test : ArrowFxSpec(spec = {
  "parMapN 4 returns to original context" {
    val mapCtxName = "parMap4"
    val mapCtx = Resource.fromExecutor { Executors.newFixedThreadPool(4, NamedThreadFactory { mapCtxName }) }

    checkAll {
      single.zip(mapCtx).use { (_single, _mapCtx) ->
        evalOn(_single) {
          threadName() shouldBe singleThreadName

          val (s1, s2, s3, s4) = parMapN(_mapCtx, threadName, threadName, threadName, threadName) { a, b, c, d -> Tuple4(a, b, c, d) }

          s1 shouldBe mapCtxName
          s2 shouldBe mapCtxName
          s3 shouldBe mapCtxName
          s4 shouldBe mapCtxName
          threadName() shouldBe singleThreadName
        }
      }
    }
  }

  "parMapN 4 returns to original context on failure" {
    val mapCtxName = "parMap4"
    val mapCtx = Resource.fromExecutor { Executors.newFixedThreadPool(4, NamedThreadFactory { mapCtxName }) }

    checkAll(Arb.int(1..4), Arb.throwable()) { choose, e ->
      single.zip(mapCtx).use { (_single, _mapCtx) ->
        evalOn(_single) {
          threadName() shouldBe singleThreadName

          Either.catch {
            when (choose) {
              1 -> parMapN(_mapCtx, suspend { e.suspend() }, suspend { never<Nothing>() }, suspend { never<Nothing>() }, suspend { never<Nothing>() }) { _, _, _, _ -> Unit }
              2 -> parMapN(_mapCtx, suspend { never<Nothing>() }, suspend { e.suspend() }, suspend { never<Nothing>() }, suspend { never<Nothing>() }) { _, _, _, _ -> Unit }
              3 -> parMapN(_mapCtx, suspend { never<Nothing>() }, suspend { never<Nothing>() }, suspend { e.suspend() }, suspend { never<Nothing>() }) { _, _, _, _ -> Unit }
              else -> parMapN(_mapCtx, suspend { never<Nothing>() }, suspend { never<Nothing>() }, suspend { never<Nothing>() }, suspend { e.suspend() }) { _, _, _, _ -> Unit }
            }
          } shouldBe Either.Left(e)

          threadName() shouldBe singleThreadName
        }
      }
    }
  }

  "parMapN 4 runs in parallel" {
    checkAll(Arb.int(), Arb.int(), Arb.int(), Arb.int()) { a, b, c, d ->
      val r = Atomic("")
      val modifyGate1 = Promise<Unit>()
      val modifyGate2 = Promise<Unit>()
      val modifyGate3 = Promise<Unit>()

      parMapN(
        suspend {
          modifyGate3.get()
          r.update { i -> "$i$a" }
        },
        suspend {
          modifyGate2.get()
          r.update { i -> "$i$b" }
          modifyGate3.complete(Unit)
        },
        suspend {
          modifyGate1.get()
          r.update { i -> "$i$c" }
          modifyGate2.complete(Unit)
        },
        suspend {
          r.set("$d")
          modifyGate1.complete(Unit)
        }
      ) { _a, _b, _c, _d ->
        Tuple4(_a, _b, _c, _d)
      }

      r.get() shouldBe "$d$c$b$a"
    }
  }

  "parMapN 4 finishes on single thread" {
    checkAll(Arb.string()) {
      single.use { ctx ->
        parMapN(ctx, threadName, threadName, threadName, threadName) { a, b, c, d -> Tuple4(a, b, c, d) }
      } shouldBe Tuple4(singleThreadName, singleThreadName, singleThreadName, singleThreadName)
    }
  }

  "Cancelling parMapN 4 cancels all participants" {
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

      val f = ForkAndForget { parMapN(loserA, loserB, loserC, loserD) { _a, _b, _c, _d -> Tuple4(_a, _b, _c, _d) } }

      s.acquireN(4) // Suspend until all racers started
      f.cancel()

      pa.get() shouldBe Pair(a, ExitCase.Cancelled)
      pb.get() shouldBe Pair(b, ExitCase.Cancelled)
      pc.get() shouldBe Pair(c, ExitCase.Cancelled)
      pd.get() shouldBe Pair(d, ExitCase.Cancelled)
    }
  }

  "parMapN 4 cancels losers if a failure occurs in one of the tasks" {
    checkAll(
      Arb.throwable(),
      Arb.element(listOf(1, 2, 3, 4)),
      Arb.int(),
      Arb.int(),
      Arb.int()
    ) { e, winningTask, a, b, c ->
      val s = Semaphore(0L)
      val pa = Promise<Pair<Int, ExitCase>>()
      val pb = Promise<Pair<Int, ExitCase>>()
      val pc = Promise<Pair<Int, ExitCase>>()

      val winner = suspend { s.acquireN(3); throw e }
      val loserA = suspend { guaranteeCase({ s.release(); never<Int>() }) { ex -> pa.complete(Pair(a, ex)) } }
      val loserB = suspend { guaranteeCase({ s.release(); never<Int>() }) { ex -> pb.complete(Pair(b, ex)) } }
      val loserC = suspend { guaranteeCase({ s.release(); never<Int>() }) { ex -> pc.complete(Pair(c, ex)) } }

      val r = Either.catch {
        when (winningTask) {
          1 -> parMapN(winner, loserA, loserB, loserC) { _, _, _, _ -> Unit }
          2 -> parMapN(loserA, winner, loserB, loserC) { _, _, _, _ -> Unit }
          3 -> parMapN(loserA, loserB, winner, loserC) { _, _, _, _ -> Unit }
          else -> parMapN(loserA, loserB, loserC, winner) { _, _, _, _ -> Unit }
        }
      }

      pa.get() shouldBe Pair(a, ExitCase.Cancelled)
      pb.get() shouldBe Pair(b, ExitCase.Cancelled)
      pc.get() shouldBe Pair(c, ExitCase.Cancelled)
      r shouldBe Either.Left(e)
    }
  }
})
