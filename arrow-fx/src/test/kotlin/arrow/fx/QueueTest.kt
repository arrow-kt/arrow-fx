package arrow.fx

import arrow.core.None
import arrow.core.Some
import arrow.core.Tuple2
import arrow.core.Tuple3
import arrow.core.Left
import arrow.core.extensions.list.traverse.traverse
import arrow.core.fix
import arrow.fx.extensions.fx
import arrow.fx.extensions.io.apply.mapN
import arrow.fx.extensions.io.applicative.applicative
import arrow.fx.extensions.io.concurrent.concurrent
import arrow.fx.extensions.io.dispatchers.dispatchers
import arrow.fx.extensions.io.monad.map
import arrow.fx.typeclasses.milliseconds
import arrow.test.UnitSpec
import arrow.test.generators.nonEmptyList
import arrow.test.generators.tuple2
import arrow.test.generators.tuple3
import arrow.test.laws.equalUnderTheLaw
import io.kotlintest.fail
import io.kotlintest.matchers.types.shouldBeInstanceOf
import io.kotlintest.properties.Gen
import io.kotlintest.properties.forAll
import io.kotlintest.shouldBe
import kotlin.coroutines.CoroutineContext

class QueueTest : UnitSpec() {

  init {

    fun IOOf<Nothing, Unit>.test(): Boolean =
      equalUnderTheLaw(IO.unit, EQ())

    fun allStrategyTests(
      label: String,
      ctx: CoroutineContext = IO.dispatchers<Nothing>().default(),
      queue: (Int) -> IO<Nothing, Queue<IOPartialOf<Nothing>, Int>>
    ) {

      "$label - make a queue the add values then retrieve in the same order" {
        forAll(Gen.nonEmptyList(Gen.int())) { l ->
          IO.fx<Nothing, Unit> {
            val q = !queue(l.size)
            !l.traverse(IO.applicative<Nothing>()) { q.offer(it) }
            val nl = !(1..l.size).toList().traverse(IO.applicative<Nothing>()) { q.take() }
            !IO.effect { nl == l.toList() }
          }.test()
        }
      }

      "$label - offer and take a number of values in the same order" {
        forAll(Gen.tuple3(Gen.int(), Gen.int(), Gen.int())) { t ->
          IO.fx<Nothing, Unit> {
            val q = !queue(3)
            !q.offer(t.a)
            !q.offer(t.b)
            !q.offer(t.c)
            val first = !q.take()
            val second = !q.take()
            val third = !q.take()
            !IO.effect { Tuple3(first, second, third) shouldBe t }
          }.test()
        }
      }

      "$label - time out taking from an empty queue" {
        IO.fx<Nothing, Unit> {
          val wontComplete = queue(10).flatMap(Queue<IOPartialOf<Nothing>, Int>::take)
          val start = !IO.effect { System.currentTimeMillis() }
          val received = !wontComplete.map { Some(it) }
            .waitFor(100.milliseconds, default = IO.just(None))
          val elapsed = !IO.effect { System.currentTimeMillis() - start }
          !IO.effect { received shouldBe None }
          !IO.effect { (elapsed >= 100) shouldBe true }
        }.test()
      }

      "$label - suspended take calls on an empty queue complete when offer calls made to queue" {
        forAll(Gen.int()) { i ->
          IO.fx<Nothing, Unit> {
            val q = !queue(3)
            val first = !q.take().fork(ctx)
            !q.offer(i)
            val res = !first.join()
            !IO.effect { res shouldBe i }
          }.test()
        }
      }

      "$label - multiple take calls on an empty queue complete when until as many offer calls made to queue" {
        forAll(Gen.tuple3(Gen.int(), Gen.int(), Gen.int())) { t ->
          IO.fx<Nothing, Unit> {
            val q = !queue(3)
            val first = !q.take().fork(ctx)
            val second = !q.take().fork(ctx)
            val third = !q.take().fork(ctx)
            !q.offer(t.a)
            !q.offer(t.b)
            !q.offer(t.c)
            val firstValue = !first.join()
            val secondValue = !second.join()
            val thirdValue = !third.join()
            !IO.effect {
              setOf(firstValue, secondValue, thirdValue) shouldBe setOf(t.a, t.b, t.c)
            }
          }.test()
        }
      }

      "$label - taking from a shutdown queue creates a QueueShutdown error" {
        forAll(Gen.int()) { i ->
          IO.fx<Nothing, Unit> {
            val q = !queue(10)
            !q.offer(i)
            !q.shutdown()
            !q.take()
          }.attempt().unsafeRunSync() == Left(QueueShutdown)
        }
      }

      "$label - offering to a shutdown queue creates a QueueShutdown error" {
        forAll(Gen.int()) { i ->
          IO.fx<Nothing, Unit> {
            val q = !queue(10)
            !q.shutdown()
            !q.offer(i)
          }.attempt().unsafeRunSync() == Left(QueueShutdown)
        }
      }

      "$label - joining a forked, incomplete take call on a shutdown queue creates a QueueShutdown error" {
        IO.fx<Nothing, Unit> {
          val q = !queue(10)
          val t = !q.take().fork(ctx)
          !q.shutdown()
          !t.join()
        }.attempt().unsafeRunSync() shouldBe Left(QueueShutdown)
      }

      "$label - create a shutdown hook completing a promise, then shutdown the queue, the promise should be completed" {
        IO.fx<Nothing, Unit> {
          val q = !queue(10)
          val p = !Promise<IOPartialOf<Nothing>, Boolean>(IO.concurrent<Nothing>())
          !(q.awaitShutdown().followedBy(p.complete(true))).fork()
          !q.shutdown()
          !p.get()
        }.test()
      }

      "$label - create a shutdown hook completing a promise twice, then shutdown the queue, both promises should be completed" {
        IO.fx<Nothing, Unit> {
          val q = !queue(10)
          val p1 = !Promise<IOPartialOf<Nothing>, Boolean>(IO.concurrent<Nothing>())
          val p2 = !Promise<IOPartialOf<Nothing>, Boolean>(IO.concurrent<Nothing>())
          !(q.awaitShutdown().followedBy(p1.complete(true))).fork()
          !(q.awaitShutdown().followedBy(p2.complete(true))).fork()
          !q.shutdown()
          !mapN(p1.get(), p2.get()) { (p1, p2) -> p1 && p2 }
        }.test()
      }

      "$label - shut it down, create a shutdown hook completing a promise, the promise should be completed immediately" {
        IO.fx<Nothing, Unit> {
          val q = !queue(10)
          !q.shutdown()
          val p = !Promise<IOPartialOf<Nothing>, Boolean>(IO.concurrent<Nothing>())
          !(q.awaitShutdown().followedBy(p.complete(true))).fork()
          !p.get()
        }.test()
      }
    }

    fun boundedStrategyTests(
      ctx: CoroutineContext = IO.dispatchers<Nothing>().default(),
      queue: (Int) -> IO<Nothing, Queue<IOPartialOf<Nothing>, Int>>
    ) {
      val label = "BoundedQueue"
      allStrategyTests(label, ctx, queue)

      "$label - time out offering to a queue at capacity" {
        IO.fx<Nothing, Unit> {
          val q = !queue(1)
          !q.offer(1)
          val start = !IO.effect { System.currentTimeMillis() }
          val wontComplete = q.offer(2)
          val received = !wontComplete.map { Some(it) }
            .waitFor(100.milliseconds, default = IO.just(None))
          val elapsed = !IO.effect { System.currentTimeMillis() - start }
          !IO.effect { received shouldBe None }
          !IO.effect { (elapsed >= 100) shouldBe true }
        }.test()
      }

      "$label - capacity must be a positive integer" {
        queue(0).attempt().suspended().fold(
          { err -> err.shouldBeInstanceOf<IllegalArgumentException>() },
          { fail("Expected Left<IllegalArgumentException>") }
        )
      }

      "$label - suspended offers called on an full queue complete when take calls made to queue" {
        forAll(Gen.tuple2(Gen.int(), Gen.int())) { t ->
          IO.fx<Nothing, Unit> {
            val q = !queue(1)
            !q.offer(t.a)
            !q.offer(t.b).fork(ctx)
            val first = !q.take()
            val second = !q.take()
            !IO.effect { Tuple2(first, second) shouldBe t }
          }.test()
        }
      }

      "$label - multiple offer calls on an full queue complete when as many take calls are made to queue" {
        forAll(Gen.tuple3(Gen.int(), Gen.int(), Gen.int())) { t ->
          IO.fx<Nothing, Unit> {
            val q = !queue(1)
            !q.offer(t.a)
            !q.offer(t.b).fork(ctx)
            !q.offer(t.c).fork(ctx)
            val first = !q.take()
            val second = !q.take()
            val third = !q.take()
            !IO.effect {
              setOf(first, second, third) shouldBe setOf(t.a, t.b, t.c)
            }
          }.test()
        }
      }

      "$label - joining a forked offer call made to a shut down queue creates a QueueShutdown error" {
        forAll(Gen.int()) { i ->
          IO.fx<Nothing, Unit> {
            val q = !queue(1)
            !q.offer(i)
            val o = !q.offer(i).fork(ctx)
            !q.shutdown()
            !o.join()
          }.attempt().unsafeRunSync() == Left(QueueShutdown)
        }
      }
    }

    fun slidingStrategyTests(
      ctx: CoroutineContext = IO.dispatchers<Nothing>().default(),
      queue: (Int) -> IO<Nothing, Queue<IOPartialOf<Nothing>, Int>>
    ) {
      val label = "SlidingQueue"
      allStrategyTests(label, ctx, queue)

      "$label - capacity must be a positive integer" {
        queue(0).attempt().suspended().fold(
          { err -> err.shouldBeInstanceOf<IllegalArgumentException>() },
          { fail("Expected Left<IllegalArgumentException>") }
        )
      }

      "$label - removes first element after offering to a queue at capacity" {
        forAll(Gen.int(), Gen.nonEmptyList(Gen.int())) { x, xs ->
          IO.fx<Nothing, Unit> {
            val q = !queue(xs.size)
            !q.offer(x)
            !xs.traverse(IO.applicative<Nothing>(), q::offer)
            val taken = !(1..xs.size).toList().traverse(IO.applicative<Nothing>()) { q.take() }
            !IO.effect {
              taken shouldBe xs.toList()
            }
          }.test()
        }
      }
    }

    fun droppingStrategyTests(
      ctx: CoroutineContext = IO.dispatchers<Nothing>().default(),
      queue: (Int) -> IO<Nothing, Queue<IOPartialOf<Nothing>, Int>>
    ) {
      val label = "DroppingQueue"

      allStrategyTests(label, ctx, queue)

      "$label - capacity must be a positive integer" {
        queue(0).attempt().suspended().fold(
          { err -> err.shouldBeInstanceOf<IllegalArgumentException>() },
          { fail("Expected Left<IllegalArgumentException>") }
        )
      }

      "$label - drops elements offered to a queue at capacity" {
        forAll(Gen.int(), Gen.int(), Gen.nonEmptyList(Gen.int())) { x, x2, xs ->
          IO.fx<Nothing, Unit> {
            val q = !queue(xs.size)
            !xs.traverse(IO.applicative<Nothing>()) { q.offer(it) }
            !q.offer(x) // this `x` should be dropped
            val taken = !(1..xs.size).toList().traverse(IO.applicative<Nothing>()) { q.take() }
            !q.offer(x2)
            val taken2 = !q.take()
            !IO.effect {
              taken.fix() + taken2 shouldBe xs.toList() + x2
            }
          }.test()
        }
      }
    }

    fun unboundedStrategyTests(
      ctx: CoroutineContext = IO.dispatchers<Nothing>().default(),
      queue: (Int) -> IO<Nothing, Queue<IOPartialOf<Nothing>, Int>>
    ) {
      allStrategyTests("UnboundedQueue", ctx, queue)
    }

    boundedStrategyTests { capacity -> Queue.bounded<IOPartialOf<Nothing>, Int>(capacity, IO.concurrent<Nothing>()).fix() }

    slidingStrategyTests { capacity -> Queue.sliding<IOPartialOf<Nothing>, Int>(capacity, IO.concurrent<Nothing>()).fix() }

    droppingStrategyTests { capacity -> Queue.dropping<IOPartialOf<Nothing>, Int>(capacity, IO.concurrent<Nothing>()).fix() }

    unboundedStrategyTests { Queue.unbounded<IOPartialOf<Nothing>, Int>(IO.concurrent<Nothing>()).fix() }
  }
}
