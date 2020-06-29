package arrow.fx.coroutines.stream.ops

import arrow.fx.coroutines.Atomic
import arrow.fx.coroutines.Promise
import arrow.fx.coroutines.Semaphore
import arrow.fx.coroutines.StreamSpec
import arrow.fx.coroutines.assertThrowable
import arrow.fx.coroutines.milliseconds
import arrow.fx.coroutines.sleep
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.append
import arrow.fx.coroutines.stream.compile
import arrow.fx.coroutines.stream.terminateOnNull
import arrow.fx.coroutines.throwable
import arrow.fx.coroutines.unit
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int

class SwitchMapTest : StreamSpec(iterations = 250, spec = {
  "flatMap equivalence when switching never occurs" {
    checkAll(Arb.stream(Arb.int())) { s ->
      val expected = s.compile().toList()
      val guard = Semaphore(1)
      // wait for inner to emit to prevent switching
      s.effectTap { guard.acquire() }
        // outer terminates, wait for last inner to emit
        .onFinalize { guard.acquire() }
        .switchMap { x -> Stream.emits(x).onFinalize { guard.release() } }
        .compile()
        .toList() shouldBe expected
    }
  }

  "inner stream finalizer always runs before switching" {
    checkAll(Arb.stream(Arb.int())) { s ->
      val ref = Atomic(true)
      s.switchMap {
        Stream.effect {
          ref.get()
        }.flatMap { released ->
          if (!released) Stream.raiseError(Exception("Previous inner switchMap stream was not yet released, but next one already started"))
          else
            Stream.effect {
              ref.set(false)
              sleep(20.milliseconds)
            }
              .onFinalize {
                sleep(100.milliseconds)
                ref.set(true)
              }
        }
      }
        .compile()
        .drain()
    }
  }

  "when primary stream terminates, inner stream continues" {
    checkAll(Arb.stream(Arb.int()), Arb.stream(Arb.int())) { s1, s2 ->
      val expected = s1.last().terminateOnNull().flatMap { s -> s2.append { Stream(s) } }.compile().toList()

      s1.switchMap { s -> Stream.sleep_(20.milliseconds).append { s2 }.append { Stream(s) } }
        .compile()
        .toList() shouldBe expected
    }
  }

  "when inner stream fails, overall stream fails" {
    checkAll(Arb.stream(Arb.int()), Arb.throwable()) { s0, e ->
      val s = Stream(0).append { s0 }
        .delayBy(25.milliseconds)
        .switchMap { Stream.raiseError<Int>(e) }
        .compile()

      assertThrowable {
        s.drain()
      } shouldBe e
    }
  }

  "when primary stream fails, overall stream fails and inner stream is terminated" {
    checkAll(Arb.throwable()) { e ->
      val semaphore = Semaphore(0)
      val s = Stream(0)
        .append { Stream.raiseError<Int>(e).delayBy(10.milliseconds) }
        .switchMap {
          Stream.effect { sleep(10.milliseconds) }.repeat().onFinalize { semaphore.release() }
        }
        .onFinalize { semaphore.acquire() }
        .compile()

      assertThrowable {
        s.drain()
      } shouldBe e
    }
  }

  "when inner stream fails, inner stream finalizer run before the primary one" {
    checkAll(Arb.stream(Arb.int()), Arb.throwable()) { s0, e ->
      val s = Stream(0).append { s0 }

      val verdict = Promise<Boolean>()
      val innerReleased = Atomic(false)

      val s2 = s.delayBy(25.milliseconds)
        .onFinalize { verdict.complete(innerReleased.get()) }
        .switchMap {
          Stream.raiseError<Int>(e).onFinalize { innerReleased.set(true) }
        }
        .attempt()
        .drain()
        .append {
          Stream.effect {
            if (verdict.get()) throw e
            else unit()
          }
        }.compile()

      assertThrowable {
        s2.drain()
      } shouldBe e
    }
  }

  "when primary stream fails, inner stream finalizer run before the primary one" {
    checkAll(Arb.throwable()) { e ->
      val verdict = Atomic(false)
      val innerReleased = Atomic(false)

      val s = Stream(1).append { Stream.sleep_(25.milliseconds) }.append { Stream.raiseError(e) }
        .onFinalize {
          val inner = innerReleased.get()
          verdict.set(inner)
        }
        .switchMap { Stream(1).repeat().onFinalize { innerReleased.set(true) } }
        .attempt()
        .drain()
        .append {
          Stream.effect {
            if (verdict.get()) throw e
            else unit()
          }
        }
        .compile()

      assertThrowable {
        s.drain()
      } shouldBe e
    }
  }
})
