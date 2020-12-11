---
layout: docs-fx
title: Arrow Fx - IO vs suspend 
permalink: /fx/io-vs-suspend/
---

### IO vs suspend

If you're coming from another language that embraces functional patterns, or have been a long time Arrow Fx user, or other FP libraries in other languages you might be familiar with `IO`.
But, how does `suspend` compare to `IO`, or why does Arrow embrace `suspend` over `IO`.

`IO<A>` models a program that can result in `A` or `Throwable`, similarly `suspend` will run inside a `Continuation<A>` which also results in `A` or `Throwable`.
So `suspend` and `IO<A>` effectively model the same concern, which is running a program into a `Result<A>` which is either an unexpected `Throwable` or `A`.

Some important characteristic of `IO` are:
 - It's a declarative data type that describes the program and allows re-running it how many times you want.
 - IO code can only be combined with other `IO` code, typically through `flatMap`, `parMapN`, etc
 
The exact same characteristic can be found in the `suspend` keyword in Kotlin:

You cannot call `suspend` code from `non-suspend` code

```kotlin:ank
suspend fun log(any: Any?): Unit = println(any)

fun otherCode(): String {
  log("some suspend code") // Doesn't compile, cannot call suspending code from non-suspending code
  return "Some Data"
}

suspend fun code(): String {
 log("Perfectly fine") // Allowed since we're inside suspend
 return "Some Data"
}
``` 

As you can see in the example above we can call `suspend fun log` only from the second function marker with `suspend` and the first function will not compile.
Here we're able to compose `suspend` code by just _binding_ directly in the enviroment, in contrast to `IO` where we'd have to _bind_ by flatMap'ing.

This basically allows us to forget about all methods like `flatMap`, `map` etc known as the [`Monad` or `Functor` hierarchy](https://wiki.haskell.org/Typeclassopedia#Introduction)
Use-cases of `suspend` are explained in [Getting Started](/fx/), [Asynchronous & Concurrent Programming](/fx/async/) and [Co-pure & mixing effects](/fx/co-pure-and-mixing-effects/).

### Goodbye `Functor`, `Applicative`, and `Monad`

`Arrow Fx Coroutines` removes the need to use the functional combinators found in the Functor, Applicative, and Monad type classes.

These combinators are instead represented as direct syntax and compile-time, and guaranteed by the Kotlin compiler that effectful computations denoted by the user can't run uncontrolled in functions denoted as pure.

The following combinators illustrate how the Functor hierarchy functions are pointless in the environment, given we can declare programs that respect the same semantics thanks to the Kotlin suspension system. Below, we'll see examples of a few of the most well-known combinators that will disappear from your day-to-day FP programming, and what they look like in Arrow Fx:

| TypeClass           | Wrapped | Fx |
|---------------------|-----------|--------------------|
| Functor.map         | `just(1).map { it + 1 }` | `1 + 1`  |
| Applicative.just    | `just(1)` | `1` |
| Applicative.mapN    | `mapN(just(1), just(2), ::Tuple2)` | `1 toT 2` |
| Applicative.tupled  | `tupled(just(1), just(2))` | `1 toT 2` |
| Monad.flatMap       | `IO.just(1).flatMap { n -> IO { n + 1 } }` | `1 + 1` |
| Monad.flatten       | `IO.just(IO.just(1))}.flatten()` | `1` |
| MonadDefer.later    | `IO.later { 1 }` | `effect { 1 }` |
| MonadDefer.defer    | `IO.defer { IO { 1 } }` | `effect { 1 }` |
| Async.effect        | `IO.effect { 1 }` | `effect { 1 }` |

This is, in general, true for effectful data types that are commutative.
