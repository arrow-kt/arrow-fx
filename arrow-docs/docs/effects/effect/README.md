---
layout: docs-fx
title: Effect
permalink: /effects/effect/
---

## Effect




<blockquote class="twitter-tweet" data-lang="en"><p lang="en" dir="ltr">But can I use callback? I love callbacks. Christmas tree style. with the lights and all....</p>&mdash; Hadi Hariri (@hhariri) <a href="https://twitter.com/hhariri/status/986652337543491586?ref_src=twsrc%5Etfw">April 18, 2018</a></blockquote>
<script async src="https://platform.twitter.com/widgets.js" charset="utf-8"></script>

If you want to use callbacks or run suspended datatypes, then `Effect` is the typeclass to use. It contains a single function `runAsync` that takes a callback and returns a new instance of the datatype. The operation will not yield a result immediately; to start running the suspended computation, you have to evaluate that new instance using its own start operator: `unsafeRunAsync` or `unsafeRunSync` for `IO`, `subscribe` or `blocking` for `Observable`, and `await` or `runBlocking` for `Deferred`.

TODO. Meanwhile you can find a short description in the [intro to typeclasses]({{ '/typeclasses/intro/' | relative_url }}).

### Data types

```kotlin:ank:replace
import arrow.reflect.*
import arrow.fx.typeclasses.*

TypeClass(Effect::class).dtMarkdownList()
```

ank_macro_hierarchy(arrow.fx.typeclasses.Effect)
