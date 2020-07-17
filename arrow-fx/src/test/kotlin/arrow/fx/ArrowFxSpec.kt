package arrow.fx

import arrow.core.test.UnitSpec
import io.kotest.property.Arb
import io.kotlintest.properties.PropertyContext
import io.kotest.property.forAll

abstract class ArrowFxSpec(
  private val iterations: Int = 350,
  spec: ArrowFxSpec.() -> Unit = {}
) : UnitSpec() {

  init {
    spec()
  }

  fun <A> forAll(
    genA: Arb<A>,
    property: PropertyContext.(A) -> Boolean
  ): Unit =
    forAll(
      iterations,
      genA,
      property
    )

  fun <A, B> forAll(
    genA: Arb<A>,
    genB: Arb<B>,
    property: PropertyContext.(A, B) -> Boolean
  ): Unit =
    forAll(
      iterations,
      genA,
      genB,
      property
    )

  fun <A, B, C> forAll(
    genA: Arb<A>,
    genB: Arb<B>,
    genC: Arb<C>,
    property: PropertyContext.(A, B, C) -> Boolean
  ): Unit =
    forAll(
      iterations,
      genA,
      genB,
      genC,
      property
    )

  fun <A, B, C, D> forAll(
    genA: Arb<A>,
    genB: Arb<B>,
    genC: Arb<C>,
    genD: Arb<D>,
    property: PropertyContext.(A, B, C, D) -> Boolean
  ): Unit =
    forAll(
      iterations,
      genA,
      genB,
      genC,
      genD,
      property
    )

  fun <A, B, C, D, E> forAll(
    genA: Arb<A>,
    genB: Arb<B>,
    genC: Arb<C>,
    genD: Arb<D>,
    genE: Arb<E>,
    property: PropertyContext.(A, B, C, D, E) -> Boolean
  ): Unit =
    forAll(
      iterations,
      genA,
      genB,
      genC,
      genD,
      genE,
      property
    )

  fun <A, B, C, D, E, F> forAll(
    genA: Arb<A>,
    genB: Arb<B>,
    genC: Arb<C>,
    genD: Arb<D>,
    genE: Arb<E>,
    genF: Arb<F>,
    property: PropertyContext.(A, B, C, D, E, F) -> Boolean
  ): Unit =
    forAll(
      iterations,
      genA,
      genB,
      genC,
      genD,
      genE,
      genF,
      property
    )
}
