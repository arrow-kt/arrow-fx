package arrow.fx

import arrow.core.NonEmptyList
import arrow.core.None
import arrow.core.Some
import arrow.core.Tuple2
import arrow.core.Tuple3
import arrow.core.Right
import arrow.core.Tuple4
import arrow.core.extensions.list.traverse.traverse
import arrow.core.extensions.nonemptylist.traverse.traverse
import arrow.core.fix
import arrow.fx.extensions.fx
import arrow.fx.extensions.io.applicative.applicative
import arrow.fx.extensions.io.concurrent.concurrent
import arrow.fx.extensions.io.dispatchers.dispatchers
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

    fun allStrategyTests(
      label: String,
      ctx: CoroutineContext = IO.dispatchers().default(),
      queue: (Int) -> IO<Queue<ForIO, Int>>
    ) {

      "$label - make a queue the add values then retrieve in the same order" {
        forAll(Gen.nonEmptyList(Gen.int())) { l ->
          IO.fx {
            val q = !queue(l.size)
            !l.traverse(IO.applicative(), q::offer)
            val nl = !(1..l.size).toList().traverse(IO.applicative()) { q.take() }
            nl.fix()
          }.equalUnderTheLaw(IO.just(l.toList()))
        }
      }

      "$label - queue can be filled at once with enough capacity" {
        forAll(Gen.nonEmptyList(Gen.int())) { l ->
          IO.fx {
            val q = !queue(l.size)
            val succeed = !q.tryOfferAll(l.toList())
            val res = !q.takeAll()
            Tuple2(succeed, res)
          }.equalUnderTheLaw(IO.just(Tuple2(true, l.toList())))
        }
      }

      "$label - queue can be filled at once over capacity with takers" {
        forAll(Gen.nonEmptyList(Gen.int())) { l ->
          IO.fx {
            val q = !queue(l.size)
            val succeed = !q.tryOfferAll(l.toList())
            val res = !q.takeAll()
            Tuple2(succeed, res)
          }.equalUnderTheLaw(IO.just(Tuple2(true, l.toList())))
        }
      }

      "$label - takeAll takes all values from a Queue" {
        forAll(Gen.nonEmptyList(Gen.int())) { l ->
          IO.fx {
            val q = !queue(l.size)
            !l.traverse(IO.applicative(), q::offer)
            !q.takeAll()
          }.equalUnderTheLaw(IO.just(l.toList()), EQ())
        }
      }

      "$label - peekAll takes all values from a Queue" {
        forAll(Gen.nonEmptyList(Gen.int())) { l ->
          IO.fx {
            val q = !queue(l.size)
            !l.traverse(IO.applicative(), q::offer)
            !q.peekAll()
          }.equalUnderTheLaw(IO.just(l.toList()), EQ())
        }
      }

      "$label - empty queue takeAll is empty" {
        forAll(Gen.positiveIntegers()) { n ->
          IO.fx {
            val q = !queue(n)
            !q.takeAll()
          }.equalUnderTheLaw(IO.just(emptyList()), EQ())
        }
      }

      "$label - empty queue peekAll is empty" {
        forAll(Gen.positiveIntegers()) { n ->
          IO.fx {
            val q = !queue(n)
            !q.peekAll()
          }.equalUnderTheLaw(IO.just(emptyList()), EQ())
        }
      }

      "$label - offer and take a number of values in the same order" {
        forAll(Gen.tuple3(Gen.int(), Gen.int(), Gen.int())) { t ->
          IO.fx {
            val q = !queue(3)
            !q.offer(t.a)
            !q.offer(t.b)
            !q.offer(t.c)
            val first = !q.take()
            val second = !q.take()
            val third = !q.take()
            Tuple3(first, second, third)
          }.equalUnderTheLaw(IO.just(t), EQ())
        }
      }

      "$label - time out taking from an empty queue" {
        IO.fx {
          val wontComplete = queue(10).flatMap(Queue<ForIO, Int>::take)
          val start = !effect { System.currentTimeMillis() }
          val received = !wontComplete.map { Some(it) }
            .waitFor(100.milliseconds, default = just(None))
          val elapsed = !effect { System.currentTimeMillis() - start }
          !effect { received shouldBe None }
          !effect { (elapsed >= 100) shouldBe true }
        }.equalUnderTheLaw(IO.unit, EQ())
      }

      "$label - suspended take calls on an empty queue complete when offer calls made to queue" {
        forAll(Gen.int()) { i ->
          IO.fx {
            val q = !queue(3)
            val first = !q.take().fork(ctx)
            !q.offer(i)
            !first.join()
          }.equalUnderTheLaw(IO.just(i), EQ())
        }
      }

      "$label - multiple take calls on an empty queue complete when until as many offer calls made to queue" {
        forAll(Gen.tuple3(Gen.int(), Gen.int(), Gen.int())) { t ->
          IO.fx {
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
            setOf(firstValue, secondValue, thirdValue)
          }.equalUnderTheLaw(IO.just(setOf(t.a, t.b, t.c)), EQ())
        }
      }

      "$label - taking from a shutdown queue creates a QueueShutdown error" {
        forAll(Gen.int()) { i ->
          IO.fx {
            val q = !queue(10)
            !q.offer(i)
            !q.shutdown()
            !q.take()
          }.equalUnderTheLaw(IO.raiseError(QueueShutdown), EQ())
        }
      }

      "$label - time out peeking from an empty queue" {
        IO.fx {
          val wontComplete = queue(10).flatMap(Queue<ForIO, Int>::peek)
          val start = !effect { System.currentTimeMillis() }
          val received = !wontComplete.map { Some(it) }
            .waitFor(100.milliseconds, default = just(None))
          val elapsed = !effect { System.currentTimeMillis() - start }
          !effect { received shouldBe None }
          !effect { (elapsed >= 100) shouldBe true }
        }.equalUnderTheLaw(IO.unit, EQ())
      }

      "$label - suspended peek calls on an empty queue complete when offer calls made to queue" {
        forAll(Gen.int()) { i ->
          IO.fx {
            val q = !queue(3)
            val first = !q.peek().fork(ctx)
            !q.offer(i)
            val res = !first.join()
            !effect { res shouldBe i }
          }.equalUnderTheLaw(IO.unit, EQ())
        }
      }

      "$label - multiple peek calls on an empty queue all complete with the first value is received" {
        forAll(Gen.int()) { i ->
          IO.fx {
            val q = !queue(1)
            val first = !q.peek().fork(ctx)
            val second = !q.peek().fork(ctx)
            val third = !q.peek().fork(ctx)
            !q.offer(i)
            val firstValue = !first.join()
            val secondValue = !second.join()
            val thirdValue = !third.join()
            !effect { setOf(firstValue, secondValue, thirdValue) shouldBe setOf(i, i, i) }
          }.equalUnderTheLaw(IO.unit, EQ())
        }
      }

      "$label - peeking a shutdown queue creates a QueueShutdown error" {
        forAll(Gen.int()) { i ->
          IO.fx {
            val q = !queue(10)
            !q.offer(i)
            !q.shutdown()
            !q.peek()
          }.equalUnderTheLaw(IO.raiseError(QueueShutdown), EQ())
        }
      }

      "$label - peek does not remove value from Queue" {
        forAll(Gen.int()) { i ->
          IO.fx {
            val q = !queue(10)
            val size1 = !q.size()
            !q.offer(i)
            val size2 = !q.size()
            val peeked = !q.peek()
            val size3 = !q.size()
            !effect { Tuple4(size1, size2, peeked, size3) shouldBe Tuple4(0, 1, i, 1) }
          }.equalUnderTheLaw(IO.unit, EQ())
        }
      }

      "$label - offering to a shutdown queue creates a QueueShutdown error" {
        forAll(Gen.int()) { i ->
          IO.fx {
            val q = !queue(10)
            !q.shutdown()
            !q.offer(i)
          }.equalUnderTheLaw(IO.raiseError(QueueShutdown), EQ())
        }
      }

      "$label - joining a forked, incomplete take call on a shutdown queue creates a QueueShutdown error" {
        IO.fx {
          val q = !queue(10)
          val t = !q.take().fork(ctx)
          !q.shutdown()
          !t.join()
        }.equalUnderTheLaw(IO.raiseError(QueueShutdown), EQ())
      }

      "$label - create a shutdown hook completing a promise, then shutdown the queue, the promise should be completed" {
        IO.fx {
          val q = !queue(10)
          val p = !Promise<ForIO, Boolean>(IO.concurrent())
          !(q.awaitShutdown().followedBy(p.complete(true))).fork()
          !q.shutdown()
          !p.get()
        }.equalUnderTheLaw(IO.just(true), EQ())
      }

      "$label - create a shutdown hook completing a promise twice, then shutdown the queue, both promises should be completed" {
        IO.fx {
          val q = !queue(10)
          val p1 = !Promise<ForIO, Boolean>(IO.concurrent())
          val p2 = !Promise<ForIO, Boolean>(IO.concurrent())
          !(q.awaitShutdown().followedBy(p1.complete(true))).fork()
          !(q.awaitShutdown().followedBy(p2.complete(true))).fork()
          !q.shutdown()
          !mapN(p1.get(), p2.get()) { (p1, p2) -> p1 && p2 }
        }.equalUnderTheLaw(IO.just(true), EQ())
      }

      "$label - shut it down, create a shutdown hook completing a promise, the promise should be completed immediately" {
        IO.fx {
          val q = !queue(10)
          !q.shutdown()
          val p = !Promise<ForIO, Boolean>(IO.concurrent())
          !(q.awaitShutdown().followedBy(p.complete(true))).fork()
          !p.get()
        }.equalUnderTheLaw(IO.just(true), EQ())
      }

      "$label - tryOffer on a shutdown Queue returns false" {
        forAll(Gen.int()) { i ->
          IO.fx {
            val q = !queue(10)
            !q.shutdown()
            val offered = !q.tryOffer(i)
            !effect { offered shouldBe false }
          }.equalUnderTheLaw(IO.unit, EQ())
        }
      }

      "$label - tryTake on a shutdown Queue returns false" {
        IO.fx {
          val q = !queue(10)
          !q.shutdown()
          val took = !q.tryTake()
          !effect { took shouldBe None }
        }.equalUnderTheLaw(IO.unit, EQ())
      }

      "$label - tryPeek on a shutdown Queue returns false" {
        IO.fx {
          val q = !queue(10)
          !q.shutdown()
          val peeked = !q.tryPeek()
          !effect { peeked shouldBe None }
        }.equalUnderTheLaw(IO.unit, EQ())
      }

      "$label - tryTake on an empty Queue returns false" {
        IO.fx {
          val q = !queue(10)
          val took = !q.tryTake()
          !effect { took shouldBe None }
        }.equalUnderTheLaw(IO.unit, EQ())
      }

      "$label - tryPeek on an empty Queue returns false" {
        IO.fx {
          val q = !queue(10)
          val peeked = !q.tryPeek()
          !effect { peeked shouldBe None }
        }.equalUnderTheLaw(IO.unit, EQ())
      }

      "$label - take is cancelable" {
        IO.fx {
          val q = !queue(1)
          val t1 = !q.take().fork()
          val t2 = !q.take().fork()
          val t3 = !q.take().fork()
          !IO.sleep(10.milliseconds) // Give take callbacks a chance to register
          !t2.cancel()
          !q.offer(1)
          !q.offer(3)
          val r1 = !t1.join()
          val r3 = !t3.join()
          !effect { setOf(r1, r3) shouldBe setOf(1, 3) }
        }.equalUnderTheLaw(IO.unit, EQ())
      }

      "$label - peek is cancelable" {
        IO.fx {
          val q = !queue(1)
          val finished = !Promise<Int>()
          val fiber = !q.peek().flatMap(finished::complete).fork()
          !IO.sleep(100.milliseconds) // Give read callback a chance to register
          !fiber.cancel()
          !q.offer(10)
          val fallback = sleep(200.milliseconds).followedBy(IO.just(0))
          val res = !IO.raceN(finished.get(), fallback)
          !effect { res shouldBe Right(0) }
        }.equalUnderTheLaw(IO.unit, EQ())
      }
    }

    fun boundedStrategyTests(
      ctx: CoroutineContext = IO.dispatchers().default(),
      queue: (Int) -> IO<Queue<ForIO, Int>>
    ) {
      val label = "BoundedQueue"
      allStrategyTests(label, ctx, queue)

      "$label - time out offering to a queue at capacity" {
        IO.fx {
          val q = !queue(1)
          !q.offer(1)
          val start = !effect { System.currentTimeMillis() }
          val wontComplete = q.offer(2)
          val received = !wontComplete.map { Some(it) }
            .waitFor(100.milliseconds, default = just(None))
          val elapsed = !effect { System.currentTimeMillis() - start }
          !effect { received shouldBe None }
          !effect { (elapsed >= 100) shouldBe true }
        }.equalUnderTheLaw(IO.unit)
      }

      "$label - time out offering multiple values to a queue at capacity" {
        IO.fx {
          val q = !queue(3)
          val start = !effect { System.currentTimeMillis() }
          val wontComplete = q.offerAll(1, 2, 3, 4)
          val received = !wontComplete.map { Some(it) }
            .waitFor(100.milliseconds, default = just(None))
          val elapsed = !effect { System.currentTimeMillis() - start }
          !effect { received shouldBe None }
          !effect { (elapsed >= 100) shouldBe true }
        }.equalUnderTheLaw(IO.unit)
      }

      "$label - queue cannot be filled at once without enough capacity" {
        forAll(Gen.nonEmptyList(Gen.int())) { l ->
          IO.fx {
            val q = !queue(l.size)
            val succeed = !q.tryOfferAll(l.toList() + 1)
            val res = !q.takeAll()
            Tuple2(succeed, res)
          }.equalUnderTheLaw(IO.just(Tuple2(false, emptyList<Int>())))
        }
      }

      "$label - can offerAll at capacity with take" {
        IO.fx {
          val q = !queue(1)
          val (join, _) = !q.take().fork()
          !IO.sleep(250.milliseconds)
          !q.offerAll(1, 2)
          val a = !q.take()
          val b = !join
          setOf(a, b)
        }.equalUnderTheLaw(IO.just(setOf(1, 2)))
      }

      "$label - can tryOfferAll at capacity with take" {
        IO.fx {
          val q = !queue(1)
          val (join, _) = !q.take().fork()
          !IO.sleep(250.milliseconds)
          val succeed = !q.tryOfferAll(1, 2)
          val a = !q.take()
          val b = !join
          Tuple2(succeed, setOf(a, b))
        }.equalUnderTheLaw(IO.just(Tuple2(true, setOf(1, 2))))
      }

      "$label - offerAll blocks when values not  taken" {
        IO.fx {
          val q = !queue(1)
          val start = !effect { System.currentTimeMillis() }
          val res = !q.offerAll(1, 2, 3).map { Some(it) }
            .waitFor(100.milliseconds, default = just(None))

          val elapsed = !effect { System.currentTimeMillis() - start }
          !effect { res shouldBe None }
          !effect { (elapsed >= 100) shouldBe true }
        }.equalUnderTheLaw(IO.unit)
      }

      // offerAll(fa).fork() + offerAll(fb).fork() <==> queue(fa + fb) OR queue(fb + fa)
      "$label - offerAll is atomic" {
        forAll(Gen.nonEmptyList(Gen.int()), Gen.nonEmptyList(Gen.int())) { fa, fb ->
          IO.fx {
            val q = !queue(fa.size + fb.size)
            !q.offerAll(fa.toList()).fork()
            !q.offerAll(fb.toList()).fork()

            !IO.sleep(50.milliseconds)

            val res = !q.takeAll()
            res == (fa.toList() + fb.toList()) || res == (fb.toList() + fa.toList())
          }.equalUnderTheLaw(IO.just(true))
        }
      }

      "$label - takeAll takes outstanding  offers" {
        forAll(50, Gen.nonEmptyList(Gen.int()).filter { it.size <= 100 }, Gen.choose(1, 100)) { l, capacity ->
          IO.fx {
            val q = !queue(capacity)
            !l.parTraverse(NonEmptyList.traverse(), q::offer).fork()
            !IO.sleep(50.milliseconds) // Give take callbacks a chance to register

            val l = !q.takeAll()
            l.toSet()
          }.equalUnderTheLaw(IO.just(l.toList().toSet()))
        }
      }

      "$label - peekAll takes outstanding  offers" {
        forAll(50, Gen.nonEmptyList(Gen.int()).filter { it.size <= 100 }, Gen.choose(1, 100)) { l, capacity ->
          IO.fx {
            val q = !queue(capacity)
            !l.parTraverse(NonEmptyList.traverse(), q::offer).fork()
            !IO.sleep(50.milliseconds) // Give take callbacks a chance to register

            val l = !q.peekAll()
            l.toSet()
          }.equalUnderTheLaw(IO.just(l.toList().toSet()))
        }
      }

      // Offer only gets scheduled for Bounded Queues, others apply strategy.
      "$label - offer is cancelable" {
        IO.fx {
          val q = !queue(1)
          !q.offer(0)
          !q.offer(1).fork()
          val p2 = !q.offer(2).fork()
          !q.offer(3).fork()

          !IO.sleep(10.milliseconds) // Give put callbacks a chance to register

          !p2.cancel()

          !q.take()
          val r1 = !q.take()
          val r3 = !q.take()

          !effect { setOf(r1, r3) shouldBe setOf(1, 3) }
        }.equalUnderTheLaw(IO.unit, EQ())
      }

      // OfferAll only gets scheduled for Bounded Queues, others apply strategy.
      "$label - offerAll is cancelable" {
        IO.fx {
          val q = !queue(1)
          !q.offer(0)
          !q.offer(1).fork()
          val p2 = !q.offerAll(2, 3).fork()
          !q.offer(4).fork()

          !IO.sleep(10.milliseconds) // Give put callbacks a chance to register

          !p2.cancel()

          !q.take()
          val r1 = !q.take()
          val r3 = !q.take()

          !effect { setOf(r1, r3) shouldBe setOf(1, 4) }
        }.equalUnderTheLaw(IO.unit, EQ())
      }

      "$label - tryOffer returns false at capacity" {
        IO.fx {
          val q = !queue(1)
          !q.offer(1)
          val offered = !q.tryOffer(2)
          !effect { offered shouldBe false }
        }.equalUnderTheLaw(IO.unit, EQ())
      }

      "$label - capacity must be a positive integer" {
        queue(0).attempt().suspended().fold(
          { err -> err.shouldBeInstanceOf<IllegalArgumentException>() },
          { fail("Expected Left<IllegalArgumentException>") }
        )
      }

      "$label - suspended offers called on an full queue complete when take calls made to queue" {
        forAll(Gen.tuple2(Gen.int(), Gen.int())) { t ->
          IO.fx {
            val q = !queue(1)
            !q.offer(t.a)
            !q.offer(t.b).fork(ctx)
            val first = !q.take()
            val second = !q.take()
            Tuple2(first, second)
          }.equalUnderTheLaw(IO.just(t), EQ())
        }
      }

      "$label - multiple offer calls on an full queue complete when as many take calls are made to queue" {
        forAll(Gen.tuple3(Gen.int(), Gen.int(), Gen.int())) { t ->
          IO.fx {
            val q = !queue(1)
            !q.offer(t.a)
            !q.offer(t.b).fork(ctx)
            !q.offer(t.c).fork(ctx)
            val first = !q.take()
            val second = !q.take()
            val third = !q.take()
            setOf(first, second, third)
          }.equalUnderTheLaw(IO.just(setOf(t.a, t.b, t.c)), EQ())
        }
      }

      "$label - joining a forked offer call made to a shut down queue creates a QueueShutdown error" {
        forAll(Gen.int()) { i ->
          IO.fx {
            val q = !queue(1)
            !q.offer(i)
            val o = !q.offer(i).fork(ctx)
            !q.shutdown()
            !o.join()
          }.equalUnderTheLaw(IO.raiseError(QueueShutdown), EQ())
        }
      }
    }

    fun slidingStrategyTests(
      ctx: CoroutineContext = IO.dispatchers().default(),
      queue: (Int) -> IO<Queue<ForIO, Int>>
    ) {
      val label = "SlidingQueue"
      allStrategyTests(label, ctx, queue)

      "$label - capacity must be a positive integer" {
        queue(0).attempt().suspended().fold(
          { err -> err.shouldBeInstanceOf<IllegalArgumentException>() },
          { fail("Expected Left<IllegalArgumentException>") }
        )
      }

      "$label - tryOffer returns false at capacity" {
        IO.fx {
          val q = !queue(1)
          !q.offer(1)
          val offered = !q.tryOffer(2)
          !effect { offered shouldBe false }
        }.equalUnderTheLaw(IO.unit, EQ())
      }

      "$label - tryOfferAll over capacity" {
        forAll(Gen.list(Gen.int()).filter { it.size > 1 }) { l ->
          IO.fx {
            val q = !queue(1)
            !q.tryOfferAll(l)
          }.equalUnderTheLaw(IO.just(false))
        }
      }

      "$label - tryOfferAll under capacity" {
        forAll(Gen.list(Gen.int()).filter { it.size <= 100 }, Gen.int().filter { it > 100 }) { l, capacity ->
          IO.fx {
            val q = !queue(capacity)
            val succeed = !q.tryOfferAll(l)
            val all = !q.takeAll()
            Tuple2(succeed, all)
          }.equalUnderTheLaw(IO.just(Tuple2(true, l)))
        }
      }

      "$label - can take and offer at capacity" {
        IO.fx {
          val q = !queue(1)
          val (join, _) = !q.take().fork()
          !IO.sleep(250.milliseconds)
          val succeed = !q.tryOfferAll(1, 2)
          val a = !q.take()
          val b = !join
          Tuple2(succeed, setOf(a, b))
        }.equalUnderTheLaw(IO.just(Tuple2(true, setOf(1, 2))))
      }

      "$label - queue cannot be filled at once without enough capacity" {
        forAll(Gen.nonEmptyList(Gen.int())) { l ->
          IO.fx {
            val q = !queue(l.size)
            val succeed = !q.tryOfferAll(l.toList() + 1)
            val res = !q.takeAll()
            Tuple2(succeed, res)
          }.equalUnderTheLaw(IO.just(Tuple2(false, emptyList<Int>())))
        }
      }

      "$label - removes first element after offering to a queue at capacity" {
        forAll(Gen.int(), Gen.nonEmptyList(Gen.int())) { x, xs ->
          IO.fx {
            val q = !queue(xs.size)
            !q.offer(x)
            !xs.traverse(IO.applicative(), q::offer)
            val taken = !(1..xs.size).toList().traverse(IO.applicative()) { q.take() }
            taken.fix()
          }.equalUnderTheLaw(IO.just(xs.toList()), EQ())
        }
      }
    }

    fun droppingStrategyTests(
      ctx: CoroutineContext = IO.dispatchers().default(),
      queue: (Int) -> IO<Queue<ForIO, Int>>
    ) {
      val label = "DroppingQueue"

      allStrategyTests(label, ctx, queue)

      "$label - capacity must be a positive integer" {
        queue(0).attempt().suspended().fold(
          { err -> err.shouldBeInstanceOf<IllegalArgumentException>() },
          { fail("Expected Left<IllegalArgumentException>") }
        )
      }

      "$label - tryOfferAll over capacity" {
        forAll(Gen.list(Gen.int()).filter { it.size > 1 }) { l ->
          IO.fx {
            val q = !queue(1)
            !q.tryOfferAll(l)
          }.equalUnderTheLaw(IO.just(false))
        }
      }

      "$label - tryOfferAll under capacity" {
        forAll(Gen.list(Gen.int()).filter { it.size <= 100 }, Gen.int().filter { it > 100 }) { l, capacity ->
          IO.fx {
            val q = !queue(capacity)
            val succeed = !q.tryOfferAll(l)
            val all = !q.takeAll()
            Tuple2(succeed, all)
          }.equalUnderTheLaw(IO.just(Tuple2(true, l)))
        }
      }

      "$label - can take and offer at capacity" {
        IO.fx {
          val q = !queue(1)
          val (join, _) = !q.take().fork()
          !IO.sleep(250.milliseconds)
          val succeed = !q.tryOfferAll(1, 2)
          val a = !q.take()
          val b = !join
          Tuple2(succeed, setOf(a, b))
        }.equalUnderTheLaw(IO.just(Tuple2(true, setOf(1, 2))))
      }

      "$label - queue cannot be filled at once without enough capacity" {
        forAll(Gen.nonEmptyList(Gen.int())) { l ->
          IO.fx {
            val q = !queue(l.size)
            val succeed = !q.tryOfferAll(l.toList() + 1)
            val res = !q.takeAll()
            Tuple2(succeed, res)
          }.equalUnderTheLaw(IO.just(Tuple2(false, emptyList<Int>())))
        }
      }

      "$label - tryOffer returns false at capacity" {
        IO.fx {
          val q = !queue(1)
          !q.offer(1)
          val offered = !q.tryOffer(5000)
          !effect { offered shouldBe false }
        }.equalUnderTheLaw(IO.unit, EQ())
      }

      "$label - drops elements offered to a queue at capacity" {
        forAll(Gen.int(), Gen.int(), Gen.nonEmptyList(Gen.int())) { x, x2, xs ->
          IO.fx {
            val q = !queue(xs.size)
            !xs.traverse(IO.applicative(), q::offer)
            !q.offer(x) // this `x` should be dropped
            val taken = !(1..xs.size).toList().traverse(IO.applicative()) { q.take() }
            !q.offer(x2)
            val taken2 = !q.take()
            taken.fix() + taken2
          }.equalUnderTheLaw(IO.just(xs.toList() + x2), EQ())
        }
      }
    }

    fun unboundedStrategyTests(
      ctx: CoroutineContext = IO.dispatchers().default(),
      queue: (Int) -> IO<Queue<ForIO, Int>>
    ) {
      allStrategyTests("UnboundedQueue", ctx, queue)
    }

    boundedStrategyTests { capacity -> Queue.bounded<ForIO, Int>(capacity, IO.concurrent()).fix() }

    slidingStrategyTests { capacity -> Queue.sliding<ForIO, Int>(capacity, IO.concurrent()).fix() }

    droppingStrategyTests { capacity -> Queue.dropping<ForIO, Int>(capacity, IO.concurrent()).fix() }

    unboundedStrategyTests { Queue.unbounded<ForIO, Int>(IO.concurrent()).fix() }
  }
}
