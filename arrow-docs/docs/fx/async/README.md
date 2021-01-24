---
layout: docs-fx
title: Arrow Fx Coroutines - Asynchronous & Concurrent Programming
permalink: /fx/async/
---

# Asynchronous & Concurrent Programming

Arrow Fx benefits from the `suspend` syntax for extremely succinct programs without callbacks.
This allows us to use direct style syntax with asynchronous and concurrent operations while preserving effect control in the types and runtime, and bind their results to the left-hand side.
The resulting expressions enjoy the same syntax that most OOP and Java programmers are already accustomed to—direct blocking imperative style.

## Parallelization & Concurrency

Arrow Fx comes with built-in versions of `parMapN`, `parTraverse`, and `parSequence` and many more allowing users to dispatch effects in parallel and receive non-blocking results and direct syntax without wrappers.
All parallel suspend operators in Arrow Fx behave in the following way.

 - When one of the parallel task fails, the others are also cancelled since a result cannot be determined. This will allow the other parallel operations to gracefully exit and close their resources before returning.

 - When the resulting suspend operation is cancelled than all running fibers inside will also be cancelled so that all paralell running task can gracefully exit and close their resources before returning.

For more documentation on parallel operations see below.

### `parMapN`/`parTupledN`

`parMapN` allows *N#* effects to run in parallel on a given `CoroutineContext` suspending until all results completed, and then apply the user-provided transformation over the results.
All input suspend functions are guaranteed to dispatch on the given CoroutineContext before they start running.
It also wires their respective cancellation. That means that cancelling the resulting suspend fun will cancel both functions running in parallel inside.
Additionally, the function does not return until both tasks are finished and their results combined by f: (A, B) -> C.

```kotlin:ank:playground
import arrow.fx.coroutines.*

//sampleStart
suspend fun threadName(): String =
  Thread.currentThread().name

data class ThreadInfo(
  val threadA: String,
  val threadB: String
)

suspend fun main(): Unit {
  val (threadA: String, threadB: String) =
    parMapN(::threadName, ::threadName, ::ThreadInfo)

  println(threadA)
  println(threadB)
}
//sampleEnd
```

### `parTraverse`

`parTraverse` allows to map elements of the same type `A` in parallel for a given `Iterable`, and then gather all the transformed results in a `List<B>`.
Cancelling the caller will cancel all running operations inside parTraverse gracefully.

```kotlin:ank:playground
import arrow.fx.coroutines.*

//sampleStart
suspend fun threadName(i: Int): String =
  "$i on ${Thread.currentThread().name}"

suspend fun main(): Unit {
  val result: List<String> = 
    listOf(1, 2, 3).parTraverse(::threadName)

  println(result)
}
//sampleEnd
```

### `raceN`

`raceN` allows *N#* effects to race in parallel and non-blocking waiting for the first results to complete, and then cancel all remaining racers.
Once the function specifies a valid return, we can observe how the returned non-blocking value is bound on the left-hand side.

```kotlin:ank:playground
import kotlinx.coroutines.delay
import arrow.fx.coroutines.raceN
import arrow.fx.coroutines.never
import kotlin.time.milliseconds
import kotlin.time.ExperimentalTime

//sampleStart
suspend fun loser(): Unit =
  never() // Never wins

@ExperimentalTime
suspend fun winner(): Int {
  delay(5.milliseconds)
  return 5
}

@ExperimentalTime
suspend fun main(): Unit {
  val res = raceN({ loser() }, { winner() })

  println(res)
}
//sampleEnd
```

## Cancellation

The cancellation system is inherited from KotlinX Coroutines and works the same.
See [KotlinX Coroutines documentation](https://kotlinlang.org/docs/reference/coroutines/cancellation-and-timeouts.html)

All operators found in Arrow Fx check for cancellation.
In the small example of an infinite loop below `parMapN` checks for cancellation and thus this function also check for cancellation before/and while sleeping.

```kotlin:ank
import kotlinx.coroutines.Dispatchers

tailrec suspend fun sleeper(): Unit {
  println("I am sleepy. I'm going to nap")
  parMapN(Dispatchers.IO, { Thread.currentThread().name }, { Thread.currentThread().name }, ::Pair)  // <-- cancellation check-point
  println("1 second nap.. Going to sleep some more")
  sleeper()
}
```

## Resource Safety

To ensure resource safety we need to take care with cancellation since we don't wont our process to be cancelled but our resources to remain open.

There Arrow Fx offers 2 tools `Resource` and `suspend fun bracketCase`. Any `resource` operations exists out of 3 steps.

1. Acquiring the resource
2. Using the resource
3. Releasing the resource with either a result, a `Throwable` or `Cancellation`.

To ensure the resource can be correctly acquired we make the `acquire` & `release` step `uncancelable`.
If the `bracketCase` was cancelled during `acquire` it'll immediately go to `release`, skipping the `use` step.

`bracketCase` is defined below, in the `release` step you can inspect the `ExitCase` of the `acquire`/`use`.

```
sealed ExitCase {
  object Completed: ExitCase()
  object Cancelled: ExitCase()
  data class Error(val error: Throwable): ExitCase()
}

suspend fun <A, B> bracketCase(acquire: suspend () -> A, use: suspend (A) -> B, release: (a, ExitCase) -> B): B
```

`bracket` is an overload of `bracketCase` that ignores the `ExitCase` value, a simple example.
We want to create a function to safely create and consume a `DatabaseConnection` that always needs to be closed no matter what the _ExitCase_.

```kotlin:ank
class DatabaseConnection {
  suspend fun open(): Unit = println("Database connection opened")
  suspend fun close(): Unit = println("Database connection closed")
}

suspend fun <A> onDbConnection(f: suspend (DatabaseConnection) -> A): A =
  bracket(
    acquire = { DatabaseConnection().apply { open() } },
    use = f,
    release = DatabaseConnection::close
  )
```

The difference between `Resource` is that `bracketCase` is simple function, while `Resource` is a data type, both ensure that resources are `acquire`d and `release`d correctly.
It also forms a `Monad` so you can use it to safely compose `Resource`s, map them or safely traverse `Resource`s.

```kotlin:ank:playground
import arrow.fx.coroutines.*

class DatabaseConnection {
  suspend fun open(): Unit = println("Database connection opened")
  suspend fun close(): Unit = println("Database connection closed")
  suspend fun query(id: String): String =
    id.toUpperCase()
}

val conn: Resource<DatabaseConnection> =
  Resource(
    { DatabaseConnection().apply { open() } },  
    DatabaseConnection::close
  )

suspend fun main(): Unit {
  val res = conn.use { db ->
    db.query("hello, world!")
  }

  println(res)
}
```

## Arrow Fx Coroutines, KotlinX Coroutines & Kotlin Standard Library
 
### Demystify Coroutine 

Kotlin's standard library defines a `Coroutine` as an instance of a suspendable computation.

In other words, a `Coroutine` is a compiled `suspend () -> A` program wired to a `Continuation`.

Which can be created by using [`kotlin.coroutines.intrinsics.createCoroutineUnintercepted`](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.coroutines.intrinsics/create-coroutine-unintercepted.html).

So let's take a quick look at an example.

```kotlin:ank:playground
import kotlin.coroutines.Continuation
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.intrinsics.createCoroutineUnintercepted
import kotlin.coroutines.resume

suspend fun one(): Int = 1

val cont: Continuation<Unit> = ::one
  .createCoroutineUnintercepted(Continuation(EmptyCoroutineContext) { println(it) })

fun main() {
  cont.resume(Unit)
}
```

As you can see here above we create a `Coroutine` using `createCoroutineUnintercepted` which returns us `Continuation<Unit>`.
Strange, you might've expected a `Coroutine` type but a `Coroutine` is represented by `Continuation<Unit>`.

This `typealias Coroutine = Contination<Unit>` will start running every time you call `resume(Unit)`, which allows you to run the suspend program N times.

## Integrating with third-party libraries

Arrow Fx integrates with KotlinX Coroutines Fx, Reactor framework, and any library that can model effectful async/concurrent computations as `suspend`.

If you are interested in the Arrow Fx library, please contact us in the main [Arrow Gitter](https://gitter.im/arrow-kt/Lobby) or #Arrow channel on the official [Kotlin Lang Slack](https://kotlinlang.slack.com/messages/C5UPMM0A0) with any questions and we'll help you along the way.
