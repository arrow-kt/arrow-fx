---
layout: docs-fx
title: Arrow Fx - Co-pure & Mixing effects 
permalink: /fx/co-pure-and-mixing-effects/
---

# Co-pure functions

Co-pure functions are pure in their definition, but allow for suspend functions to be mixed.
Let's take a well-known example, `List.map`. The definition from the Kotlin Standard library is the following:

```kotlin:ank
inline fun <A, B> List<A>.map(transform: (A) -> B): List<B> {
    val destination = ArrayList(this.size)
    for (item in this)
        destination.add(transform(item))
    return destination
}
```

Since this function is defined as `inline`, there will be no actual function invocation but rather the code will be _inlined_ at the call site.
So `listOf(1, 2).map { it + 1 }` in bytecode, is exactly the same as if we would have manually created the `ArrayList`, looped over it while applying the function and collecting the results. 
So the function doesn't really exist at bytecode, so we call it _co-pure_ since it's pure in definition but if it's actually pure depends on the actual usage.

```kotlin:ank
suspend fun log(a: Any): Unit =
  println(a)

suspend fun program(): Unit =
  listOf(1, 2, 3, 4).map { log(it) }
```

In this example we can see that the `suspend fun log` can be safely from within the `suspend fun program()`, which would not be possible if `map` was not inlined. 

So _inline_ allows us to safely call `suspend` code from within the expected lambdas.
Which is a powerful feature which allows us to combine `suspend` functions with pure data types such as `List`.

## Mixing effects

In Kotlin we can mix effects using `suspend` which is not possible with wrapper `Monad`s which require monad transformers.
Whether you're familiar with monad transfomers is not important, since Arrow Core and Arrow Fx Coroutines offers an alternative to them which is ad-hoc mixing of effects.

Probably the best known effect is `Either`, which models the _effect_ of `Left` or `Right` or branching in a type. Check the Arrow Core docs for more information on `Either` [here](https://arrow-kt.io/docs/apidocs/arrow-core-data/arrow.core/-either/).
We already covered how we can mix it with suspend using _inline_ or _co-pure_ functions, but we can also provide a DSL which eliminates nesting of lambdas.
It looks like `monad comphrensions` if you're familiar with them from other languages but it works quite different under the hood.

Let's imagine the following set of functions and domain:

```kotlin:ank
sealed class UniversityError
object StudentNotFound: UniversityError()
object UniversityNotFound: UniversityError() 
object DeanNotFound: UniversityError()

data class Student(val name String, val universityId: Int)
data class University(val name String, val deanId: Int)
data class Dean(val name String, val deanId: Int)

suspend fun getStudentFromDatabase(name: String): Either<StudentNotFound, Student> =
  Student(name, 1).right()
 
suspend fun getUniversityFromDatabase(universityId: Int): Either<UniversityNotFound, University> =
  University("Lambda University", 1).right()

suspend fun getDeanFromDatabase(deanId: Int): Either<DeanNotFound, Dean> =
  Dean("Prof. Arrow", deanId)
```

We'd write the following nested code:

```kotlin:ank
suspend fun getAllInfo(name: String): Either<UniversityError, Triple<Student, University, Dean>> = 
  getStudentFromDatabase(name).flatMap { student ->
    getUniversityFromDatabase(student.id).flatMap { university ->
       getDeanFromDatabase(university.deanId).map { dean ->
         Triple(student, univeristy, dean)
       }
    }
  }
```

However, Arrow Core allows us to mix computation blocks with suspending code so that we can write it in a more elegant and imperative way:

```kotlin:ank
suspend fun betterGetAllInfo(name: String): Either<UniversityError, Triple<Student, University, Dean>> = 
  either {
    val student = getStudentFromDatabase(name)()
    val university = getUniversityFromDatabase(student.id)()
    val dean = getDeanFromDatabase(university.deanId)()
    Triple(student, univeristy, dean)
  }
``` 

Arrow Core offers computation blocks for your favorite data types which are all mix-able with suspend as we shown here with `Either`.
