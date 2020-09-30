---
layout: docs-fx
title: Arrow Fx - Handling concurrent state
permalink: /fx/managing-state/
---

# Handling concurrent state

Fx programs have a variety of options available when it comes to managing state in a concurrent environment.

## Problems with shared state in a concurrent environment

The problem of having shared state throughout an application is consistency when this state is read and updated.
An invariant checked a few lines ago may now no longer be valid if other threads changed the variables that were read.
Updating a variable based on a prior read may lead to inconsistent state if there has been another update in-between...

These issues are referred to as race-conditions and are the source of many headaches and lots of time spent debugging.

In this article we will explore how arrow fx helps to deal with this issue and in some cases can even avoid it entirely.

## Accessing a single shared variable

When we are accessing only a single variable we can use [Atomic](TODO Link).
This gives us access to a single mutable variable where reads and writes are atomic operations,
 meaning no other access to the variable can happen during any operation on an atomic variable.
 
```kotlin:ank:playground
import arrow.fx.coroutines.*

suspend fun main() {
  val count = Atomic(0)
  (0 until 20_000).parTraverse {
    count.update(Int::inc)
  }
  println(count.get())
}
```

In this example `count.update` is our atomic operation, and it is performed by possibly many threads at the same time.
Despite that our counter increments one by one and never contains an invalid state (like say two threads incrementing 1 to 2 at the same time and both persisting 2 missing one update).

### Drawbacks of using atomic variables

Atomic variables have one major drawback: Their atomic guarantees do not scale beyond one variable.
We can guarantee that access to one variable is atomic, but accessing two variables with atomic operations, even right after each other, will not make the entire operation atomic.

## Securing access to shared state with locks

One solution to the problem of modifying multiple variables of shared state is to surround the access with taking and releasing a lock.
The general idea is to section off a piece of code that only one single thread can access at a time.

In arrow fx we offer [ConcurrentVar](TODO link) as a datatype to build locking code with.
A `ConcurrentVar` can either be empty or full and its operations `read` and `write` operations (called `take` and `put`) block if the variable is empty, for reads, or full, for writes.

With a `ConcurrentVar` our example becomes:
```kotlin:ank:playground
import arrow.fx.coroutines.*

suspend fun main(): Unit {
  val mvar = ConcurrentVar(0)
  (0 until 20_000).parTraverse {
    val current = mvar.take()
    // <1>
    mvar.put(current + 1)
  }
  println(mvar.take())
}
```

When a thread tries to invoke `ConcurrentVar.take()` it will suspend until the `ConcurrentVar` has a value to take and then removes the value.
This means, after taking, no other thread can finish a call to `ConcurrentVar.take()` until `ConcurrentVar.put()` is called putting a value in the `ConcurrentVar`.
This means that the section between `take` and `put` is never run by more than one thread at a time.
In our example this section is marked as `<1>`.

This offers a few ways to deal with shared state now:
- We can use one global `ConcurrentVar` to store our state and thus force all access to our state to first `take` the variable and then to `put` it back when we are done modifying.
- We can also split our state into more fine-grained variables to allow for more concurrency.

## Drawbacks

A major drawback to locks is that they are tricky to use correctly:
- Forgot to release a lock? Any future attempt at taking it will fail, and that thread will block forever!
- Forgot to take a lock? Hello race-conditions again!
- Take multiple locks in the wrong order? Now we may enter a scenario where two threads wait on each other to make progress and release their locks, but neither can because the other holds the lock the thread is waiting for.
This is called a deadlock and will block those threads forever.

Especially the last two issues are terribly hard to debug and avoiding such a situation entirely depends on in-depth knowledge of where locks are taken and in which order, which is quite the mental overhead and hurts new developers.

Using a single lock avoids most of these issues except that now we have sections of code that only one thread at a time can make progress in.
This is not great for concurrency and will slow our program down.

## Transactions

Because using locks is hard, and using a single lock or a single shared reference is usually not good enough, arrow fx offers one alternative method of accessing shared state: `Software-transactional-memory` (from now on [STM](TODO link))

The general idea is to write our state access as transactions which offer a guarantee of consistency.

With transactions our example becomes:
```kotlin:ank:playground
import arrow.fx.coroutines.*
import arrow.fx.coroutines.stm.TVar

suspend fun main(): Unit {
  val tvar = TVar.new(0)
  (0 until 20_000).parTraverse {
    atomically { tvar.modify { it + 1 } }
  }
  println(tvar.unsafeRead())
}
```

Our safe access section now becomes everything inside the `atomically` block.

While this does not look too different from our previous examples `atomically` provides a few improvements:
- Any read of a `TVar` performed inside `atomically` will always return the same value when we eventually commit the transaction, that is to say the value has not changed when we perform the effects of a transaction.
- This extends to any number of reads on any number of `TVar`'s in a single transaction.
- Writes are local to the transaction until we have validated consistency and commit.

To achieve this `STM` imposes exactly one restriction on transactions: They need to be pure (in the context of `STM`).
See the downsides further down as to why this is the case.

This example nicely shows how transactions compose and can be used to update multiple variables:
```kotlin:ank:playground
import arrow.fx.coroutines.Environment
import arrow.fx.coroutines.atomically
import arrow.fx.coroutines.stm.TVar
import arrow.fx.coroutines.STM

//sampleStart
suspend fun STM.transfer(from: TVar<Int>, to: TVar<Int>, amount: Int): Unit {
  withdraw(from, amount)
  deposit(to, amount)
}
suspend fun STM.deposit(acc: TVar<Int>, amount: Int): Unit {
  val current = acc.read()
  acc.write(current + amount)
  // or the shorthand acc.modify { it + amount }
}
suspend fun STM.withdraw(acc: TVar<Int>, amount: Int): Unit {
  val current = acc.read()
  if (current - amount >= 0) acc.write(current + amount)
  else throw IllegalStateException("Not enough money in the account!")
}
//sampleEnd
fun main() {
  Environment().unsafeRunSync {
    val acc1 = TVar.new(500)
    val acc2 = TVar.new(300)
    println("Balance account 1: ${acc1.unsafeRead()}")
    println("Balance account 2: ${acc2.unsafeRead()}")
    println("Performing transaction")
    atomically { transfer(acc1, acc2, 50) }
    println("Balance account 1: ${acc1.unsafeRead()}")
    println("Balance account 2: ${acc2.unsafeRead()}")
  }
}
```

For `transfer` it is very important for the transaction to have a consistent state, otherwise we might end up with an account that has an amount less than 0, which our invariant is trying to prevent!

One of the best things about transactions is that they compose and scale.
Regardless of how many variables are being read, or how many threads you throw at executing them, a transaction will always commit with a consistent state.

Another interesting bit about transactions is that independent transactions will not block each other.
Two transactions may commit in parallel if they do not write to the same `TVar`'s.
Note that read only access will always allow concurrency and never leads to any locks taken.
> This is an implementation detail of our `STM` implementation and may not hold for other implementations.
> Our implementation uses fine-grained locks to maximize concurrency, whereas some other implementations use global locks and thus do not scale as well.
> For example Haskell (GHC) implements both a global lock and a fine-grained lock version, which it chooses between at compile time.

All in all `STM` transactions are a very powerful tool which enables writing stateful concurrent code in a painless and safe way.

### Downsides

When a transaction tries to commit its changes it will validate that the state of each read `TVar` is still consistent with what the transaction expects.
If that is not the case it will simply throw away all changes and start over.
This effectively means that if we were to do side effects inside a transaction those may be performed many times.

In most use cases this is a non issue, and the benefits of transactions far outweigh this downside.

There are two other important downsides of `STM` to note:
- Transactions are worse under high contention.
If a variable is updated many many times a second it may be beneficial to use a different concurrency tool like atomic references or locks.
- Transactions have no notion of fairness. If two threads wait for a lock to be released, the one who came first will get the lock next.
This does not happen with transactions. If multiple transactions retry and block (ie they wait for a variable to change), they will all resume at the same time when a variable changes.
The fastest transaction always wins, which makes it possible to starve long-running transactions forever.

These downsides are tradeoffs and for most use cases do not apply, but it is still important to be aware of them!
