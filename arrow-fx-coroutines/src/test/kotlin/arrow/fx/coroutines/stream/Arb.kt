package arrow.fx.coroutines.stream

import arrow.fx.coroutines.prependTo
import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.Sample
import io.kotest.property.Shrinker
import io.kotest.property.arbitrary.arb
import io.kotest.property.arbitrary.bool
import io.kotest.property.arbitrary.byte
import io.kotest.property.arbitrary.choice
import io.kotest.property.arbitrary.choose
import io.kotest.property.arbitrary.create
import io.kotest.property.arbitrary.double
import io.kotest.property.arbitrary.float
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.long
import io.kotest.property.arbitrary.map
import io.kotest.property.arbitrary.set
import io.kotest.property.arbitrary.short
import kotlin.random.nextInt

@JvmOverloads
inline fun <reified A> Arb.Companion.array(
  gen: Arb<A>,
  range: IntRange = 0..100
): Arb<Array<A>> {
  check(!range.isEmpty())
  check(range.first >= 0)
  return arb(edgecases = emptyArray<A>() prependTo gen.edgecases().map { arrayOf(it) }) {
    sequence {
      val genIter = gen.generate(it).iterator()
      while (true) {
        val targetSize = it.random.nextInt(range)
        val list = ArrayList<A>(targetSize)
        while (list.size < targetSize && genIter.hasNext()) {
          list.add(genIter.next().value)
        }
        check(list.size == targetSize)
        yield(list.toArray() as Array<A>)
      }
    }
  }
}

@PublishedApi
internal fun <A, B> arrayChunkGenerator(
  arb: Arb<A>,
  shrinker: Shrinker<B>,
  range: IntRange = 0..10,
  build: (values: List<A>, offset: Int, length: Int) -> B
): Arb<B> {
  check(!range.isEmpty())
  check(range.first >= 0)

  val edgecases =
    arb.edgecases().map { a -> build(listOf(a), 0, 1) } + build(emptyList(), 0, 0)

  return arb(edgecases, shrinker) {
    val genIter = arb.generate(it).iterator()

    sequence {
      while (true) {
        val targetSize = it.random.nextInt(range)
        val list = ArrayList<A>(targetSize)

        while (list.size < targetSize && genIter.hasNext()) {
          list.add(genIter.next().value)
        }

        val offset = (0..list.size).random(it.random)
        val length = (0..(list.size - offset)).random(it.random)

        yield(build(list, offset, length))
      }
    }
  }
}

class ChunkShrinker<A> : Shrinker<Chunk<A>> {
  override fun shrink(value: Chunk<A>): List<Chunk<A>> =
    if (value.isEmpty()) emptyList()
    else listOf(
      Chunk.empty(),
      value.takeLast(1),
      value.take(value.size() / 3),
      value.take(value.size() / 2),
      value.take(value.size() * 2 / 3),
      value.dropLast(1)
    )
}

inline fun <reified A> Arb.Companion.chunk(arb: Arb<A>): Arb<Chunk<A>> =
  Arb.choose(
    1 to Arb.create { Chunk.empty<A>() },
    5 to arb.map { Chunk.just(it) },
    10 to Arb.list(arb, 0..20).map { Chunk.iterable(it) },
    10 to Arb.set(arb, 0..20).map { Chunk.iterable(it) },
    10 to Arb.array(arb, 0..20).map { Chunk.array(it) },
    10 to Arb.boxedChunk(arb)
  )

inline fun <reified A> Arb.Companion.boxedChunk(arb: Arb<A>): Arb<Chunk<A>> =
  arrayChunkGenerator(arb, ChunkShrinker()) { values, offset, length ->
    Chunk.boxed(values.toTypedArray(), offset, length)
  }

fun Arb.Companion.booleanChunk(): Arb<Chunk<Boolean>> =
  Arb.choice(
    arrayChunkGenerator(Arb.bool(), ChunkShrinker()) { values, offset, length ->
      Chunk.booleans(values.toBooleanArray(), offset, length)
    },
    arrayChunkGenerator(Arb.bool(), ChunkShrinker()) { values, _, _ ->
      Chunk.array(values.toTypedArray())
    }
  )

fun Arb.Companion.byteChunk(): Arb<Chunk<Byte>> =
  Arb.choice(
    arrayChunkGenerator(Arb.byte(), ChunkShrinker()) { values, offset, length ->
      Chunk.bytes(values.toByteArray(), offset, length)
    },
    arrayChunkGenerator(Arb.byte(), ChunkShrinker()) { values, _, _ ->
      Chunk.array(values.toTypedArray())
    }
  )

fun Arb.Companion.intChunk(): Arb<Chunk<Int>> =
  Arb.choice(
    arrayChunkGenerator(Arb.int(), ChunkShrinker()) { values, offset, length ->
      Chunk.ints(values.toIntArray(), offset, length)
    },
    arrayChunkGenerator(Arb.int(), ChunkShrinker()) { values, _, _ ->
      Chunk.array(values.toTypedArray())
    }
  )

fun Arb.Companion.longChunk(): Arb<Chunk<Long>> =
  Arb.choice(
    arrayChunkGenerator(Arb.long(), ChunkShrinker()) { values, offset, length ->
      Chunk.longs(values.toLongArray(), offset, length)
    },
    arrayChunkGenerator(Arb.long(), ChunkShrinker()) { values, _, _ ->
      Chunk.array(values.toTypedArray())
    }
  )

fun Arb.Companion.doubleChunk(): Arb<Chunk<Double>> =
  Arb.choice(
    arrayChunkGenerator(Arb.double(), ChunkShrinker()) { values, offset, length ->
      Chunk.doubles(values.toDoubleArray(), offset, length)
    },
    arrayChunkGenerator(Arb.double(), ChunkShrinker()) { values, _, _ ->
      Chunk.array(values.toTypedArray())
    }
  )

fun Arb.Companion.floatChunk(): Arb<Chunk<Float>> =
  Arb.choice(
    arrayChunkGenerator(Arb.float(), ChunkShrinker()) { values, offset, length ->
      Chunk.floats(values.toFloatArray(), offset, length)
    },
    arrayChunkGenerator(Arb.float(), ChunkShrinker()) { values, _, _ ->
      Chunk.array(values.toTypedArray())
    }
  )

fun Arb.Companion.shortChunk(): Arb<Chunk<Short>> =
  Arb.choice(
    arrayChunkGenerator(Arb.short(), ChunkShrinker()) { values, offset, length ->
      Chunk.shorts(values.toShortArray(), offset, length)
    },
    arrayChunkGenerator(Arb.short(), ChunkShrinker()) { values, _, _ ->
      Chunk.array(values.toTypedArray<Short>())
    }
  )
