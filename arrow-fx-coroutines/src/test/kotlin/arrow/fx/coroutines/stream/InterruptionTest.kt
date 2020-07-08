package arrow.fx.coroutines.stream

import arrow.core.*
import arrow.fx.coroutines.*
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int

class InterruptionTest : StreamSpec(spec = {
  "can cancel a hung effect" - {
    checkAll(Arb.stream(Arb.int())) { s ->
      val latch = Promise<Unit>()
      val exit = Promise<ExitCase>()

      val f = ForkAndForget {
        s.append { Stream(1) } // Make sure is not empty
          .effectMap {
            guaranteeCase({ latch.complete(Unit); never<Unit>() }) { ex -> exit.complete(ex) }
          }.interruptWhen { Right(latch.get().also { sleep(20.milliseconds) }) }
          .compile()
          .toList()
      }

      latch.get()
      f.cancel()
      timeOutOrNull(50.milliseconds) { exit.get() } shouldBe ExitCase.Cancelled
    }
  }

  "can interrupt a hung effect" - {
    checkAll(Arb.stream(Arb.int())) { s ->
      s.effectMap { never<Unit>() }
        .interruptWhen { Right(sleep(20.milliseconds)) }
        .compile()
        .toList() shouldBe emptyList()
    }
  }

  "termination successful when stream doing interruption is hung" - {
    checkAll(Arb.stream(Arb.int())) { s ->
      Stream.effect { Semaphore(0) }
        .flatMap { semaphore ->
          val interrupt = Stream(true).append { Stream.effect_ { semaphore.release() } }

          s.effectMap { semaphore.acquire() }
            .interruptWhen(interrupt)
        }
        .compile()
        .toList() shouldBe emptyList()
    }
  }

  "constant stream" - {
    checkAll(Arb.int()) { i ->
      Stream.constant(i)
        .interruptWhen { Right(sleep(20.milliseconds)) }
        .compile()
        .drain() // Finishes and gets interrupted
    }
  }

  "constant stream with a flatMap" - {
    checkAll(Arb.int()) { i ->
      Stream.constant(i)
        .interruptWhen { Right(sleep(20.milliseconds)) }
        .flatMap { Stream(1) }
        .compile()
        .drain()
    }
  }

  "infinite recursive stream" - {
    fun loop(i: Int): Stream<Int> =
      Stream(i).flatMap { i -> Stream(i).append { loop(i + 1) } }

    loop(0)
      .interruptWhen { Right(sleep(20.milliseconds)) }
      .compile()
      .drain()
  }

  "infinite recursive stream that never emits" - {
    fun loop(): Stream<Int> =
      Stream.effect { Unit }.flatMap { loop() }

    loop()
      .interruptWhen { Right(sleep(20.milliseconds)) }
      .compile()
      .drain()
  }

  "infinite recursive stream that never emits and has no effect" - {
    fun loop(): Stream<Int> =
      Stream(Unit).flatMap { loop() }

    loop()
      .interruptWhen { Right(sleep(20.milliseconds)) }
      .compile()
      .drain()
  }

  "effect stream" - {
    Stream.effect { Unit }.repeat()
      .interruptWhen { Right(sleep(20.milliseconds)) }
      .compile()
      .drain()
  }

  "Constant drained stream" - {
    Stream.constant(true)
      .interruptWhen { Right(sleep(20.milliseconds)) }
      .compile()
      .drain()
  }

  "terminates when interruption stream is infinitely false" - {
    checkAll(Arb.stream(Arb.int())) { s ->
      val expected = s.compile().toList()
      s.interruptWhen(Stream.constant(false))
        .compile()
        .toList() shouldBe expected
    }
  }

  "both streams hung" - {
    checkAll(Arb.stream(Arb.int())) { s ->
      val barrier = Semaphore(0)
      val enableInterrupt = Semaphore(0)
      val interrupt = Stream.effect { enableInterrupt.acquire() }.flatMap { Stream(false) }
      s.effectMap { i ->
        // enable interruption and hang when hitting a value divisible by 7
        if (i % 7 == 0) {
          enableInterrupt.release()
          barrier.acquire()
          i
        } else i
      }.interruptWhen(interrupt)
        .compile()
        // as soon as we hit a value divisible by 7, we enable interruption then hang before emitting it,
        // so there should be no elements in the output that are divisible by 7
        // this also checks that interruption works fine even if one or both streams are in a hung state
        .toList().forEach { it % 7 shouldNotBe 0 }
    }
  }

  "stream that never terminates in flatMap" - {
    checkAll(Arb.stream(Arb.int())) { s ->
      s.interruptWhen { Right(sleep(20.milliseconds)) }
        .flatMap { Stream.never<Int>() }
        .compile()
        .toList() shouldBe emptyList()
    }
  }

  "failure from interruption signal will be propagated to main stream even when flatMap stream is hung" - {
    checkAll(Arb.stream(Arb.int()), Arb.throwable()) { s, e ->
      Either.catch {
        Stream.effect { Semaphore(0) }.flatMap { semaphore ->
          Stream(1)
            .append { s }
            .interruptWhen { sleep(20.milliseconds); Either.Left(e) }
            .flatMap { Stream.effect_ { semaphore.acquire() } }
        }
          .compile()
          .toList()
      } shouldBe Either.Left(e)
    }
  }

  "resume on append" - {
    Stream.never<Unit>()
      .interruptWhen { Right(sleep(20.milliseconds)) }
      .append { Stream(5) }
      .compile()
      .toList() shouldBe listOf(5)
  }

  "hang in effectMap and then resume on append" - {
    checkAll(Arb.stream(Arb.int())) { s ->
      val expected = s.compile().toList()

      s.interruptWhen { Right(sleep(20.milliseconds)) }
        .effectMap { never<Int>() }
        .drain()
        .append { s }
        .compile()
        .toList() shouldBe expected
    }
  }

  "effectMap + filterOption and then resume on append" - {
    checkAll(Arb.stream(Arb.int())) { s ->
      val expected = s.compile().toList()

      s.interruptWhen { Right(sleep(20.milliseconds)) }
        .effectMap { never<Option<Int>>() }
        .append { s.map { Some(it) } }
        .filterOption()
        .compile()
        .toList() shouldBe expected
    }
  }

  "interruption works when flatMap is followed by filterOption" - {
    checkAll(Arb.stream(Arb.int())) { s ->
      val expected = s.compile().toList()

      s.append { Stream(1) }
        .interruptWhen { Right(sleep(50.milliseconds)) }
        .map { None }
        .append { s.map { Some(it) } }
        .flatMap {
          when (it) {
            None -> Stream.never<Option<Int>>()
            is Some -> Stream(it)
          }
        }
        .filterOption()
        .compile()
        .toList() shouldBe expected
    }
  }

  "if a pipe is interrupted, it will not restart evaluation" - {
    checkAll(Arb.int(1..100)) { n ->
      val latch = Promise<Unit>()

      val p: Pipe<Int, Int> = Pipe {
        fun loop(acc: Int, pull: Pull<Int, Unit>): Pull<Int, Unit> =
          pull.uncons1OrNull().flatMap { uncons1 ->
            when (uncons1) {
              null -> Pull.output1(acc)
              else -> Pull.output1(uncons1.head).flatMap {
                val stop = if (uncons1.head == n) Pull.effect { latch.complete(Unit) } else Pull.done
                stop.flatMap { loop(acc + uncons1.head, uncons1.tail) }
              }
            }
          }

        loop(0, it.asPull()).stream()
      }

      Stream.iterate(0, Int::inc)
        .flatMap { Stream(it) }
        .interruptWhen { Right(latch.get()) }
        .through(p)
        .compile()
        .toList()
        .let { result ->
          result shouldBe listOfNotNull(result.firstOrNull()) + result.drop(1).filter { it != 0 }
        }
    }
  }

  "resume on append with pull" - {
    Stream(1)
      .unchunk()
      .interruptWhen { Right(sleep(20.milliseconds)) }
      .asPull()
      .unconsOrNull()
      .flatMap { uncons ->
        when (uncons) {
          null -> Pull.done
          else -> Pull.effect { never<Unit>() }
        }
      }
      .stream()
      .interruptScope()
      .append { Stream(5) }
      .compile()
      .toList() shouldBe listOf(5)
  }

  "resume with append after evalMap interruption" - {
    Stream(1)
      .interruptWhen { Right(sleep(20.milliseconds)) }
      .effectMap { never<Int>() }
      .append { Stream(5) }
      .compile()
      .toList() shouldBe listOf(5)
  }

  "interrupted effect is cancelled" - {
    val latch = Promise<Unit>()

    timeOutOrNull(500.milliseconds) {
      Stream.effect { guarantee(latch::get) { latch.complete(Unit) } }
        .interruptAfter(50.milliseconds)
        .compile()
        .drain()

      latch.get()
      true
    } shouldBe true
  }

  "nested-interrupt" - {
    io.kotest.property.checkAll(500, Arb.stream(Arb.int())) { s ->
      val expected = s.compile().toList()

      s.interruptWhen { Right(sleep(50.milliseconds)) }
        .map { None }
        .append { s.map { Option(it) } }
        .interruptWhen { Right(never()) }
        .flatMap {
          when (it) {
            is None -> Stream.effect { never<Nothing>(); None }
            is Some -> Stream(Some(it.t))
          }
        }.filterOption()
        .compile()
        .toList() shouldBe expected
    }
  }

  "nested-interrupt - interrupt in outer scope interrupts the inner scope" - {
    Stream.effect { never<Unit>() }
      .interruptWhen { never() }
      .interruptWhen { Right(Unit) }
      .compile()
      .toList() shouldBe emptyList()
  }

  "nested-interrupt - interrupt in enclosing scope recovers" - {
    Stream.effect { never<Unit>() }
      .interruptWhen { never() }
      .append { Stream(1).delayBy(20.milliseconds) }
      .interruptWhen { Right(Unit) }
      .append { Stream(2) }
      .compile()
      .toList() shouldBe listOf(2)
  }

  "Compiled stream can be cancelled" - {
    val stop = Promise<ExitCase>()
    val latch = Promise<Unit>()

    val fiber = ForkAndForget {
      guaranteeCase({
        latch.complete(Unit)
        Stream.never<Unit>()
          .compile()
          .resource
          .drain()
          .use { Unit }
      }, { ex -> stop.complete(ex) })
    }

    latch.get()
    fiber.cancel()

    stop.get() shouldBe ExitCase.Cancelled
  }
})
