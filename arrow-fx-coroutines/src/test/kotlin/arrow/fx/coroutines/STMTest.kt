package arrow.fx.coroutines

import arrow.fx.coroutines.stm.TQueue
import arrow.fx.coroutines.stm.TVar
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.bool
import io.kotest.property.arbitrary.int

class STMTest : ArrowFxSpec(spec = {
  "no-effects" {
    atomically { 10 } shouldBeExactly 10
  }
  "reading from vars" {
    checkAll(Arb.int()) { i: Int ->
      val tv = TVar.new(i)
      atomically { tv.read() } shouldBeExactly i
      tv.unsafeRead() shouldBeExactly i
    }
  }
  "reading and writing" {
    checkAll(Arb.int(), Arb.int()) { i: Int, j: Int ->
      val tv = TVar.new(i)
      atomically { tv.write(j) }
      tv.unsafeRead() shouldBeExactly j
    }
  }
  "read after a write should have the updated value" {
    checkAll(Arb.int(), Arb.int()) { i: Int, j: Int ->
      val tv = TVar.new(i)
      atomically { tv.write(j); tv.read() } shouldBeExactly j
      tv.unsafeRead() shouldBeExactly j
    }
  }
  "reading multiple variables" {
    checkAll(Arb.int(), Arb.int(), Arb.int()) { i: Int, j: Int, k: Int ->
      val v1 = TVar.new(i)
      val v2 = TVar.new(j)
      val v3 = TVar.new(k)
      atomically { v1.read() + v2.read() + v3.read() } shouldBeExactly i + j + k
      v1.unsafeRead() shouldBeExactly i
      v2.unsafeRead() shouldBeExactly j
      v3.unsafeRead() shouldBeExactly k
    }
  }
  "reading and writing multiple variables" {
    checkAll(Arb.int(), Arb.int(), Arb.int()) { i: Int, j: Int, k: Int ->
      val v1 = TVar.new(i)
      val v2 = TVar.new(j)
      val v3 = TVar.new(k)
      val sum = TVar.new(0)
      atomically {
        val s = v1.read() + v2.read() + v3.read()
        sum.write(s)
      }
      v1.unsafeRead() shouldBeExactly i
      v2.unsafeRead() shouldBeExactly j
      v3.unsafeRead() shouldBeExactly k
      sum.unsafeRead() shouldBeExactly i + j + k
    }
  }
  "retry without prior reads throws an exception" {
    shouldThrow<BlockedIndefinitely> { atomically { retry() } }
  }
  "retry should suspend forever if no read variable changes" {
    timeOutOrNull(500.milliseconds) {
      val tv = TVar.new(0)
      atomically {
        if (tv.read() == 0) retry()
        else 200
      }
    } shouldBe null
  }
  "a suspended transaction will resume if a variable changes" {
    val tv = TVar.new(0)
    val f = ForkConnected {
      sleep(500.milliseconds)
      atomically { tv.modify { it + 1 } }
    }
    atomically {
      when (val i = tv.read()) {
        0 -> retry()
        else -> i
      }
    } shouldBeExactly 1
    f.join()
  }
  "a suspended transaction will resume if any variable changes" {
    val v1 = TVar.new(0)
    val v2 = TVar.new(0)
    val v3 = TVar.new(0)
    val f = ForkConnected {
      sleep(500.milliseconds)
      atomically { v1.modify { it + 1 } }
      sleep(500.milliseconds)
      atomically { v2.modify { it + 1 } }
      sleep(500.milliseconds)
      atomically { v3.modify { it + 1 } }
    }
    atomically {
      val i = v1.read() + v2.read() + v3.read()
      check(i >= 3)
      i
    } shouldBeExactly 3
    f.join()
  }
  "retry + orElse: retry orElse t1 = t1" {
    atomically {
      stm { retry() } orElse { 10 }
    } shouldBeExactly 10
  }
  "retry + orElse: t1 orElse retry = t1" {
    atomically {
      stm { 10 } orElse { retry() }
    } shouldBeExactly 10
  }
  "retry + orElse: associativity" {
    checkAll(Arb.bool(), Arb.bool(), Arb.bool()) { b1: Boolean, b2: Boolean, b3: Boolean ->
      if ((b1 || b2 || b3).not()) {
        shouldThrow<BlockedIndefinitely> {
          atomically {
            stm { stm { check(b1) } orElse { check(b2) } } orElse { check(b3) }
          }
        } shouldBe shouldThrow {
          atomically {
            stm { check(b1) } orElse { stm { check(b2) } orElse { check(b3) } }
          }
        }
      } else {
        atomically {
          stm { stm { check(b1) } orElse { check(b2) } } orElse { check(b3) }
        } shouldBe atomically {
          stm { check(b1) } orElse { stm { check(b2) } orElse { check(b3) } }
        }
      }
    }
  }
  "suspended transactions are resumed for variables accessed in orElse" {
    checkAll {
      val tv = TVar.new(0)
      val f = ForkConnected {
        sleep(10.microseconds)
        atomically { tv.modify { it + 1 } }
      }
      atomically {
        stm {
          when (val i = tv.read()) {
            0 -> retry()
            else -> i
          }
        } orElse { retry() }
      } shouldBeExactly 1
      f.join()
    }
  }
  "on a single variable concurrent transactions should be linear" {
    checkAll {
      val tv = TVar.new(0)
      val res = TQueue.new<Int>()
      val fibers = mutableListOf<Fiber<Any?>>()
      for (i in 0..100) {
        fibers.add(
          ForkConnected {
            atomically {
              val r = tv.read().also { tv.write(it + 1) }
              res.write(r)
            }
          }
        )
      }
      fibers.forEach { it.join() }
      atomically { res.flush() } shouldBe (0..100).toList()
    }
  }
  "atomically rethrows exceptions" {
    shouldThrow<IllegalArgumentException> { atomically { throw IllegalArgumentException("Test") } }
  }
})
