package arrow.fx.coroutines

import arrow.fx.coroutines.stm.TVar

suspend fun STM.f(): Int {
  retry()
}

class STMTest : ArrowFxSpec(spec = {
  "basic transaction" {
    val tv = TVar.new("Hello")
    parMapN({
      atomically {
        val read = tv.read()

        val res = stm {
          retry()
        } orElse {
          10
        }
        println(res)
        if (read == "Hello") retry()
      }
    }, {
      atomically {
        tv.write("Weird")
      }
    }, { _, _ -> Unit })

    println(tv.unsafeRead())
  }
})
