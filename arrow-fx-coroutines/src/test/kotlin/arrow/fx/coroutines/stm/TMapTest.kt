package arrow.fx.coroutines.stm

import arrow.fx.coroutines.ArrowFxSpec
import arrow.fx.coroutines.atomically
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.pair

class TMapTest : ArrowFxSpec(spec = {
  "insert values" {
    checkAll(Arb.int(), Arb.int()) { k, v ->
      val map = TMap.new<Int, Int>()
      atomically { map.insert(k, v) }
      atomically { map.lookup(k) } shouldBe v
    }
  }
  "insert multiple values" {
    checkAll(Arb.list(Arb.pair(Arb.int(), Arb.int()))) { pairs ->
      val map = TMap.new<Int, Int>()
      atomically {
        for ((k, v) in pairs) map.insert(k, v)
      }
      atomically {
        for ((k, v) in pairs) map.lookup(k) shouldBe v
      }
    }
  }
  "insert and remove" {
    checkAll(Arb.int(), Arb.int()) { k, v ->
      val map = TMap.new<Int, Int>()
      atomically { map.insert(k, v) }
      atomically { map.lookup(k) } shouldBe v
      atomically { map.remove(k) }
      atomically { map.lookup(k) } shouldBe null
    }
  }
  "update" {
    checkAll(Arb.int(), Arb.int(), Arb.int()) { k, v, g ->
      val map = TMap.new<Int, Int>()
      atomically { map.insert(k, v) }
      atomically { map.lookup(k) } shouldBe v
      atomically { map.update(k) { v + g } }
      atomically { map.lookup(k) } shouldBe v + g
    }
  }
})
