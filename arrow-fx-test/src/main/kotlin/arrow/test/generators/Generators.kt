package arrow.test.generators

import arrow.Kind
import arrow.core.Either
import arrow.core.Id
import arrow.core.Ior
import arrow.core.Left
import arrow.core.ListK
import arrow.core.MapK
import arrow.core.NonEmptyList
import arrow.core.Option
import arrow.core.Right
import arrow.core.SequenceK
import arrow.core.SetK
import arrow.core.SortedMapK
import arrow.core.Tuple2
import arrow.core.Tuple3
import arrow.core.Tuple4
import arrow.core.Tuple5
import arrow.core.Validated
import arrow.core.extensions.sequence.functorFilter.filterMap
import arrow.core.extensions.sequencek.apply.apply
import arrow.core.extensions.sequencek.functorFilter.filterMap
import arrow.core.k
import arrow.core.toOption
import arrow.typeclasses.Applicative
import arrow.typeclasses.ApplicativeError
import io.kotlintest.properties.Gen
import java.util.concurrent.TimeUnit

fun <F, A> Gen<A>.applicative(AP: Applicative<F>): Gen<Kind<F, A>> =
  map { AP.just(it) }

fun <F, A, E> Gen.Companion.applicativeError(genA: Gen<A>, errorGen: Gen<E>, AP: ApplicativeError<F, E>): Gen<Kind<F, A>> =
  Gen.oneOf<Either<E, A>>(genA.map(::Right), errorGen.map(::Left)).map {
    it.fold(AP::raiseError, AP::just)
  }

fun <F, A> Gen<A>.applicativeError(AP: ApplicativeError<F, Throwable>): Gen<Kind<F, A>> =
  Gen.applicativeError(this, Gen.throwable(), AP)

fun <F, A, E> Gen<E>.raiseError(AP: ApplicativeError<F, E>): Gen<Kind<F, A>> =
  map { AP.raiseError<A>(it) }

fun <A, B> Gen.Companion.functionAToB(gen: Gen<B>): Gen<(A) -> B> = gen.map { b: B -> { _: A -> b } }

fun <A> Gen.Companion.functionToA(gen: Gen<A>): Gen<() -> A> = gen.map { a: A -> { a } }

fun Gen.Companion.throwable(): Gen<Throwable> = Gen.from(listOf(RuntimeException(), NoSuchElementException(), IllegalArgumentException()))

fun Gen.Companion.fatalThrowable(): Gen<Throwable> = Gen.from(listOf(ThreadDeath(), StackOverflowError(), OutOfMemoryError(), InterruptedException()))

fun Gen.Companion.intSmall(): Gen<Int> = Gen.oneOf(Gen.choose(Int.MIN_VALUE / 10000, -1), Gen.choose(0, Int.MAX_VALUE / 10000))

fun <A, B> Gen.Companion.tuple2(genA: Gen<A>, genB: Gen<B>): Gen<Tuple2<A, B>> = Gen.bind(genA, genB) { a: A, b: B -> Tuple2(a, b) }

fun <A, B, C> Gen.Companion.tuple3(genA: Gen<A>, genB: Gen<B>, genC: Gen<C>): Gen<Tuple3<A, B, C>> =
  Gen.bind(genA, genB, genC) { a: A, b: B, c: C -> Tuple3(a, b, c) }

fun <A, B, C, D> Gen.Companion.tuple4(genA: Gen<A>, genB: Gen<B>, genC: Gen<C>, genD: Gen<D>): Gen<Tuple4<A, B, C, D>> =
  Gen.bind(genA, genB, genC, genD) { a: A, b: B, c: C, d: D -> Tuple4(a, b, c, d) }

fun <A, B, C, D, E> Gen.Companion.tuple5(genA: Gen<A>, genB: Gen<B>, genC: Gen<C>, genD: Gen<D>, genE: Gen<E>): Gen<Tuple5<A, B, C, D, E>> =
  Gen.bind(genA, genB, genC, genD, genE) { a: A, b: B, c: C, d: D, e: E -> Tuple5(a, b, c, d, e) }

fun Gen.Companion.nonZeroInt(): Gen<Int> = Gen.int().filter { it != 0 }

fun Gen.Companion.intPredicate(): Gen<(Int) -> Boolean> =
  Gen.nonZeroInt().flatMap { num ->
    val absNum = Math.abs(num)
    Gen.from(listOf<(Int) -> Boolean>(
      { it > num },
      { it <= num },
      { it % absNum == 0 },
      { it % absNum == absNum - 1 })
    )
  }

fun <B> Gen.Companion.option(gen: Gen<B>): Gen<Option<B>> =
  gen.orNull().map { it.toOption() }

fun <E, A> Gen.Companion.either(genE: Gen<E>, genA: Gen<A>): Gen<Either<E, A>> {
  val genLeft = genE.map<Either<E, A>> { Left(it) }
  val genRight = genA.map<Either<E, A>> { Right(it) }
  return Gen.oneOf(genLeft, genRight)
}

fun <E, A> Gen.Companion.validated(genE: Gen<E>, genA: Gen<A>): Gen<Validated<E, A>> =
  Gen.either(genE, genA).map { Validated.fromEither(it) }

fun <A> Gen.Companion.nonEmptyList(gen: Gen<A>): Gen<NonEmptyList<A>> =
  gen.flatMap { head -> Gen.list(gen).map { NonEmptyList(head, it) } }

fun <K : Comparable<K>, V> Gen.Companion.sortedMapK(genK: Gen<K>, genV: Gen<V>): Gen<SortedMapK<K, V>> =
  Gen.bind(genK, genV) { k: K, v: V -> sortedMapOf(k to v) }.map { it.k() }

fun <K, V> Gen.Companion.mapK(genK: Gen<K>, genV: Gen<V>): Gen<MapK<K, V>> =
  Gen.map(genK, genV).map { it.k() }

fun Gen.Companion.timeUnit(): Gen<TimeUnit> = Gen.from(TimeUnit.values())

fun <A> Gen.Companion.listK(genA: Gen<A>): Gen<ListK<A>> = Gen.list(genA).map { it.k() }

fun <A> Gen.Companion.sequenceK(genA: Gen<A>): Gen<SequenceK<A>> = Gen.list(genA).map { it.asSequence().k() }

fun <A> Gen.Companion.genSetK(genA: Gen<A>): Gen<SetK<A>> = Gen.set(genA).map { it.k() }

fun Gen.Companion.unit(): Gen<Unit> =
  create { Unit }

fun <T> Gen.Companion.id(gen: Gen<T>): Gen<Id<T>> = object : Gen<Id<T>> {
  override fun constants(): Iterable<Id<T>> =
    gen.constants().map { Id.just(it) }

  override fun random(): Sequence<Id<T>> =
    gen.random().map { Id.just(it) }
}

fun <A, B> Gen.Companion.ior(genA: Gen<A>, genB: Gen<B>): Gen<Ior<A, B>> =
  object : Gen<Ior<A, B>> {
    override fun constants(): Iterable<Ior<A, B>> =
      (genA.orNull().constants().asSequence().k() to genB.orNull().constants().asSequence().k()).let { (ls, rs) ->
        SequenceK.apply().run { ls.product(rs) }.filterMap {
          Ior.fromOptions(Option.fromNullable(it.a), Option.fromNullable(it.b))
        }.asIterable()
      }

    override fun random(): Sequence<Ior<A, B>> =
      (Gen.option(genA).random() to Gen.option(genB).random()).let { (ls, rs) ->
        ls.zip(rs).filterMap {
          Ior.fromOptions(it.first, it.second)
        }
      }
  }
