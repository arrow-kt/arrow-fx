package arrow.fx

import arrow.Kind
import arrow.core.extensions.eq
import arrow.core.extensions.list.traverse.traverse
import arrow.core.extensions.monoid
import arrow.core.test.UnitSpec
import arrow.core.test.generators.GenK
import arrow.core.test.laws.MonadLaws
import arrow.core.test.laws.MonoidLaws
import arrow.fx.extensions.io.applicative.applicative
import arrow.fx.extensions.io.bracket.bracket
import arrow.fx.extensions.resource.applicative.applicative
import arrow.fx.extensions.resource.functor.functor
import arrow.fx.extensions.resource.monad.monad
import arrow.fx.extensions.resource.monoid.monoid
import arrow.fx.extensions.resource.selective.selective
import arrow.fx.test.eq.eqK
import arrow.fx.test.laws.forFew
import arrow.fx.typeclasses.Bracket
import arrow.typeclasses.Eq
import arrow.typeclasses.EqK
import io.kotlintest.properties.Gen

class ResourceTest : UnitSpec() {
  init {

    val EQ: Eq<Kind<Kind<Kind<ForResource, Kind<ForIO, Nothing>>, Throwable>, Int>> =
      Resource.eqK<Nothing>().liftEq(Int.eq())

    testLaws(
      MonadLaws.laws(
        Resource.monad(IO.bracket()),
        Resource.functor(IO.bracket()),
        Resource.applicative(IO.bracket()),
        Resource.selective(IO.bracket()),
        Resource.genK(IO.bracket()),
        Resource.eqK()
      ),
      MonoidLaws.laws(Resource.monoid(Int.monoid(), IO.bracket()), Gen.int().map { Resource.just(it, IO.bracket()) }, EQ)
    )

    "Resource releases resources in reverse order of acquisition" {
      forFew(5, Gen.list(Gen.string())) { l ->
        val released = mutableListOf<String>()
        l.traverse(Resource.applicative(IO.bracket())) {
          Resource({ IO { it } }, { r -> IO { released.add(r); Unit } }, IO.bracket())
        }.fix().use { IO.unit }.fix().unsafeRunSync()

        l == released.reversed()
      }
    }
  }
}

fun <E> Resource.Companion.eqK(EQE: Eq<E> = Eq.any()) =
  object : EqK<ResourcePartialOf<IOPartialOf<E>, Throwable>> {
    override fun <A> Kind<ResourcePartialOf<IOPartialOf<E>, Throwable>, A>.eqK(other: Kind<ResourcePartialOf<IOPartialOf<E>, Throwable>, A>, EQ: Eq<A>): Boolean =
      (this.fix() to other.fix()).let {
        IO.eqK(EQE).liftEq(EQ).run {
          val ls = it.first.use(IO.Companion::just)
          val rs = it.second.use(IO.Companion::just)
          ls.eqv(rs)
        }
      }
  }

fun <F> Resource.Companion.genK(BF: Bracket<F, Throwable>): GenK<ResourcePartialOf<F, Throwable>> =
  object : GenK<ResourcePartialOf<F, Throwable>>, Bracket<F, Throwable> by BF {
    override fun <A> genK(gen: Gen<A>): Gen<Kind<ResourcePartialOf<F, Throwable>, A>> {
      val allocate = gen.map { Resource({ just(it) }, { _ -> unit() }, this) }

      return Gen.oneOf(
        // Allocate
        allocate,
        // Suspend
        allocate.map { Resource.Suspend(just(it), this) },
        // Bind
        allocate.map { it.flatMap { a -> just(a, this) } }
      )
    }
  }
