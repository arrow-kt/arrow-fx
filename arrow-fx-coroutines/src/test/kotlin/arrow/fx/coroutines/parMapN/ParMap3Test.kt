package arrow.fx.coroutines.parMapN

import arrow.core.Either
import arrow.fx.coroutines.ArrowFxSpec
import arrow.fx.coroutines.Atomic
import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.NamedThreadFactory
import arrow.fx.coroutines.Resource
import arrow.fx.coroutines.guaranteeCase
import arrow.fx.coroutines.leftException
import arrow.fx.coroutines.never
import arrow.fx.coroutines.parMapN
import arrow.fx.coroutines.single
import arrow.fx.coroutines.singleThreadName
import arrow.fx.coroutines.suspend
import arrow.fx.coroutines.threadName
import arrow.fx.coroutines.throwable
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.property.Arb
import io.kotest.property.arbitrary.element
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.string
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.util.concurrent.Executors

class ParMap3Test : ArrowFxSpec(spec = {
  "parMapN 3 returns to original context" {
    val mapCtxName = "parMap3"
    val mapCtx = Resource.fromExecutor { Executors.newFixedThreadPool(3, NamedThreadFactory { mapCtxName }) }

    checkAll {
      single.zip(mapCtx).use { (_single, _mapCtx) ->
        withContext(_single) {
          threadName() shouldBe singleThreadName

          val (s1, s2, s3) = parMapN(_mapCtx, threadName, threadName, threadName) { a, b, c -> Triple(a, b, c) }

          s1 shouldBe mapCtxName
          s2 shouldBe mapCtxName
          s3 shouldBe mapCtxName
          threadName() shouldBe singleThreadName
        }
      }
    }
  }

  "parMapN 3 returns to original context on failure" {
    val mapCtxName = "parMap3"
    val mapCtx = Resource.fromExecutor { Executors.newFixedThreadPool(3, NamedThreadFactory { mapCtxName }) }

    checkAll(Arb.int(1..3), Arb.throwable()) { choose, e ->
      single.zip(mapCtx).use { (_single, _mapCtx) ->
        withContext(_single) {
          threadName() shouldBe singleThreadName

          Either.catch {
            when (choose) {
              1 -> parMapN(_mapCtx, suspend { e.suspend() }, suspend { never<Nothing>() }, suspend { never<Nothing>() }) { _, _, _ -> Unit }
              2 -> parMapN(_mapCtx, suspend { never<Nothing>() }, suspend { e.suspend() }, suspend { never<Nothing>() }) { _, _, _ -> Unit }
              else -> parMapN(_mapCtx, suspend { never<Nothing>() }, suspend { never<Nothing>() }, suspend { e.suspend() }) { _, _, _ -> Unit }
            }
          } should leftException(e)

          threadName() shouldBe singleThreadName
        }
      }
    }
  }

  "parMapN 3 runs in parallel" {
    checkAll(Arb.int(), Arb.int(), Arb.int()) { a, b, c ->
      val r = Atomic("")
      val modifyGate1 = CompletableDeferred<Unit>()
      val modifyGate2 = CompletableDeferred<Unit>()

      parMapN(
        {
          modifyGate2.await()
          r.update { i -> "$i$a" }
        },
        {
          modifyGate1.await()
          r.update { i -> "$i$b" }
          modifyGate2.complete(Unit)
        },
        {
          r.set("$c")
          modifyGate1.complete(Unit)
        }
      ) { _a, _b, _c ->
        Triple(_a, _b, _c)
      }

      r.get() shouldBe "$c$b$a"
    }
  }

  "parMapN 3 finishes on single thread" {
    checkAll(Arb.string()) {
      single.use { ctx ->
        parMapN(ctx, threadName, threadName, threadName) { a, b, c -> Triple(a, b, c) }
      } shouldBe Triple("single", "single", "single")
    }
  }

  "Cancelling parMapN 3 cancels all participants" {
    checkAll(Arb.int(), Arb.int(), Arb.int()) { a, b, c ->
      val latchA = CompletableDeferred<Unit>()
      val latchB = CompletableDeferred<Unit>()
      val latchC = CompletableDeferred<Unit>()
      val pa = CompletableDeferred<Pair<Int, ExitCase>>()
      val pb = CompletableDeferred<Pair<Int, ExitCase>>()
      val pc = CompletableDeferred<Pair<Int, ExitCase>>()

      val loserA = suspend { guaranteeCase({ latchA.complete(Unit); never<Int>() }) { ex -> pa.complete(Pair(a, ex)) } }
      val loserB = suspend { guaranteeCase({ latchB.complete(Unit); never<Int>() }) { ex -> pb.complete(Pair(b, ex)) } }
      val loserC = suspend { guaranteeCase({ latchC.complete(Unit); never<Int>() }) { ex -> pc.complete(Pair(c, ex)) } }

      val job = launch { parMapN(loserA, loserB, loserC) { _a, _b, _c -> Triple(_a, _b, _c) } }

      latchA.await(); latchB.await(); latchC.await() // Suspend until all racers started
      job.cancel()

      pa.await().let { (res, exit) ->
        res shouldBe a
        exit.shouldBeInstanceOf<ExitCase.Cancelled>()
      }
      pb.await().let { (res, exit) ->
        res shouldBe b
        exit.shouldBeInstanceOf<ExitCase.Cancelled>()
      }
      pc.await().let { (res, exit) ->
        res shouldBe c
        exit.shouldBeInstanceOf<ExitCase.Cancelled>()
      }
    }
  }

  "parMapN 3 cancels losers if a failure occurs in one of the tasks" {
    checkAll(
      Arb.throwable(),
      Arb.element(listOf(1, 2, 3)),
      Arb.int(),
      Arb.int()
    ) { e, winningTask, a, b ->
      val latchA = CompletableDeferred<Unit>()
      val latchB = CompletableDeferred<Unit>()
      val pa = CompletableDeferred<Pair<Int, ExitCase>>()
      val pb = CompletableDeferred<Pair<Int, ExitCase>>()

      val winner = suspend { latchA.await(); latchB.await(); throw e }
      val loserA = suspend { guaranteeCase({ latchA.complete(Unit); never<Int>() }) { ex -> pa.complete(Pair(a, ex)) } }
      val loserB = suspend { guaranteeCase({ latchB.complete(Unit); never<Int>() }) { ex -> pb.complete(Pair(b, ex)) } }

      val r = Either.catch {
        when (winningTask) {
          1 -> parMapN(winner, loserA, loserB) { _, _, _ -> Unit }
          2 -> parMapN(loserA, winner, loserB) { _, _, _ -> Unit }
          else -> parMapN(loserA, loserB, winner) { _, _, _ -> Unit }
        }
      }

      pa.await().let { (res, exit) ->
        res shouldBe a
        exit.shouldBeInstanceOf<ExitCase.Cancelled>()
      }
      pb.await().let { (res, exit) ->
        res shouldBe b
        exit.shouldBeInstanceOf<ExitCase.Cancelled>()
      }
      r should leftException(e)
    }
  }
})
