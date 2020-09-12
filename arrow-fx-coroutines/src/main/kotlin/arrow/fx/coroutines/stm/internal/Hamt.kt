package arrow.fx.coroutines.stm.internal

import arrow.fx.coroutines.STM
import arrow.fx.coroutines.stm.TVar

/**
 * Low level stm datastructure which can be used to efficiently implement other datastructures like Map/Set on top.
 *
 * Based on http://lampwww.epfl.ch/papers/idealhashtrees.pdf and https://hackage.haskell.org/package/stm-hamt.
 */
data class Hamt<A>(val branches: TVar<Array<Branch<A>?>>) {
  companion object {
    suspend fun <A> new(): Hamt<A> = Hamt(TVar.new(arrayOfNulls(ARR_SIZE)))
  }
}

suspend inline fun <A> STM.lookupHamtWithHash(hmt: Hamt<A>, hash: Int): A? {
  var depth = 0
  var hamt = hmt
  while (true) {
    val branchInd = hash.indexAtDepth(depth)
    val branches = hamt.branches.read()
    when (val branch = branches[branchInd]) {
      null -> return null
      is Branch.Leaf -> return@lookupHamtWithHash branch.value
      is Branch.Branches -> {
        depth = depth.nextDepth()
        hamt = branch.sub
      }
    }
  }
}

suspend fun <A> STM.pair(depth: Int, hash1: Int, branch1: Branch<A>, hash2: Int, branch2: Branch<A>): Hamt<A> {
  val branchInd1 = hash1.indexAtDepth(depth)
  val branchInd2 = hash2.indexAtDepth(depth)
  val branches = arrayOfNulls<Branch<A>>(ARR_SIZE)
  if (branchInd1 == branchInd2) {
    val deeper = pair(depth.nextDepth(), hash1, branch1, hash2, branch2)
    branches[branchInd1] = Branch.Branches(deeper)
  } else {
    branches[branchInd1] = branch1
    branches[branchInd2] = branch2
  }
  return Hamt(newTVar(branches))
}

suspend fun <A> STM.clearHamt(hamt: Hamt<A>): Unit = hamt.branches.write(arrayOfNulls(ARR_SIZE))

suspend inline fun <reified A> STM.alterHamtWithHash(hamt: Hamt<A>, hash: Int, fn: (A?) -> A?): Boolean {
  var depth = 0
  var hmt = hamt
  while (true) {
    val branchInd = hash.indexAtDepth(depth)
    val branches = hmt.branches.read()
    when (val branch = branches[branchInd]) {
      null -> {
        val el = fn(null) ?: return false
        val new = branches.copyOf()
        new[branchInd] = Branch.Leaf(hash, el)
        hmt.branches.write(new)
        return true
      }
      is Branch.Leaf -> {
        if (hash == branch.hash) {
          val newEl = fn(branch.value)
          if (newEl == null) {
            val new = branches.copyOf()
            new[branchInd] = null
            hmt.branches.write(new)
          } else {
            val new = branches.copyOf()
            new[branchInd] = Branch.Leaf(hash, newEl)
            hmt.branches.write(new)
          }
          return true
        } else {
          val el = fn(null) ?: return false
          val newHamt = pair(
            depth.nextDepth(),
            hash,
            Branch.Leaf(hash, el),
            branch.hash,
            branch
          )
          val new = branches.copyOf()
          new[branchInd] = Branch.Branches(newHamt)
          hmt.branches.write(new)
          return true
        }
      }
      is Branch.Branches -> {
        depth = depth.nextDepth()
        hmt = branch.sub
      }
    }
  }
}

suspend fun <A> STM.newHamt(): Hamt<A> = Hamt(newTVar(arrayOfNulls(ARR_SIZE)))

sealed class Branch<out A> {
  data class Branches<A>(val sub: Hamt<A>): Branch<A>()
  data class Leaf<A>(val hash: Int, val value: A): Branch<A>()
}

const val ARR_SIZE = 32 // 2^DEPTH_STEP
const val DEPTH_STEP = 5
const val MASK = 1.shl(DEPTH_STEP) - 1

fun Int.index(): Int = MASK.and(this)
fun Int.atDepth(d: Int): Int = shr(d)
fun Int.indexAtDepth(d: Int): Int = atDepth(d).index()
fun Int.nextDepth(): Int = this + DEPTH_STEP
