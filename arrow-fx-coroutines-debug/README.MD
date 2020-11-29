# Arrow Fx Coroutines Debug

Inspired by KotlinX Coroutines Debug.

It can enabled by the system property `arrow.fx.coroutines.debug` by setting it to `on`, `off` or `auto`.
Auto is the default setting and will automatically enable debug support if the [`ea` flag](https://docs.oracle.com/cd/E19683-01/806-7930/assert-4/index.html) is found,
this flag is also automatically enabled in tests.

For following snippet:

```kotlin
ForkConnected(CoroutineName("Outer fiber") + ComputationPool) {
  parTupledN(
    CoroutineName("parTupledN") + ComputationPool,
    { ForkConnected(CoroutineName("Inner fiber") + ComputationPool) { never<Unit>() } },
    { sleep(1000.milliseconds) },
    {
      while (true) {
        cancelBoundary()
      }
    }
  )
}
```

It prints the following after everything started running.

```
Coroutines dump 2020/11/29 20:32:25
   
Coroutine Outer fiber#1:FiberContinuation@387a4647, state: SUSPENDED
    at arrow.fx.coroutines.DebugProbesTest$example$1$1.invokeSuspend(DebugProbesTest.kt:178)
    (Coroutine creation stacktrace)
    at kotlin.coroutines.intrinsics.IntrinsicsKt__IntrinsicsJvmKt.createCoroutineUnintercepted(IntrinsicsJvm.kt:87)
    at arrow.fx.coroutines.StartCoroutineCancellableKt.startCoroutineCancellable(startCoroutineCancellable.kt:33)
    at arrow.fx.coroutines.Predef_testKt$withDebugProbe$1.invokeSuspend(predef-test.kt:18)
    at kotlin.coroutines.jvm.internal.BaseContinuationImpl.resumeWith(ContinuationImpl.kt:33)
    at arrow.fx.coroutines.ExecutorServiceContinuation$resumeWith$1.run(dispatchers.kt:62)
    at java.util.concurrent.ForkJoinTask$RunnableExecuteAction.exec(ForkJoinTask.java:1402)
    at java.util.concurrent.ForkJoinTask.doExec(ForkJoinTask.java:289)
    at java.util.concurrent.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1056)
    at java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1692)
    at java.util.concurrent.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:157)

Coroutine parTupledN#3:FiberContinuation@361391c2, state: SUSPENDED
    at arrow.fx.coroutines.TimerKt.sleep(timer.kt:41)
    at arrow.fx.coroutines.DebugProbesTest$example$1$1$2.invokeSuspend(DebugProbesTest.kt:177)
    (Coroutine creation stacktrace)
    at kotlin.coroutines.intrinsics.IntrinsicsKt__IntrinsicsJvmKt.createCoroutineUnintercepted(IntrinsicsJvm.kt:87)
    at arrow.fx.coroutines.StartCoroutineCancellableKt.startCoroutineCancellable(startCoroutineCancellable.kt:33)
    at arrow.fx.coroutines.DebugProbesTest$example$1$1.invokeSuspend(DebugProbesTest.kt:174)
    at kotlin.coroutines.jvm.internal.BaseContinuationImpl.resumeWith(ContinuationImpl.kt:33)
    at arrow.fx.coroutines.ExecutorServiceContinuation$resumeWith$1.run(dispatchers.kt:62)
    at java.util.concurrent.ForkJoinTask$RunnableExecuteAction.exec(ForkJoinTask.java:1402)
    at java.util.concurrent.ForkJoinTask.doExec(ForkJoinTask.java:289)
    at java.util.concurrent.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1056)
    at java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1692)
    at java.util.concurrent.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:157)

Coroutine Inner fiber#4:FiberContinuation@bb7bf75, state: SUSPENDED
    at arrow.fx.coroutines.DebugProbesTest$example$1$1$1$1.invokeSuspend(DebugProbesTest.kt:176)
    (Coroutine creation stacktrace)
    at kotlin.coroutines.intrinsics.IntrinsicsKt__IntrinsicsJvmKt.createCoroutineUnintercepted(IntrinsicsJvm.kt:87)
    at arrow.fx.coroutines.StartCoroutineCancellableKt.startCoroutineCancellable(startCoroutineCancellable.kt:33)
    at arrow.fx.coroutines.DebugProbesTest$example$1$1$1.invokeSuspend(DebugProbesTest.kt:176)
    at kotlin.coroutines.jvm.internal.BaseContinuationImpl.resumeWith(ContinuationImpl.kt:33)
    at arrow.fx.coroutines.ExecutorServiceContinuation$resumeWith$1.run(dispatchers.kt:62)
    at java.util.concurrent.ForkJoinTask$RunnableExecuteAction.exec(ForkJoinTask.java:1402)
    at java.util.concurrent.ForkJoinTask.doExec(ForkJoinTask.java:289)
    at java.util.concurrent.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1056)
    at java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1692)
    at java.util.concurrent.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:157)

Coroutine parTupledN#5:FiberContinuation@6a6ad876, state: RUNNING
    at kotlin.jvm.internal.Intrinsics.areEqual(Intrinsics.java:167)
    at kotlin.coroutines.CoroutineContext$Element$DefaultImpls.get(CoroutineContext.kt:67)
    at kotlin.coroutines.AbstractCoroutineContextElement.get(CoroutineContextImpl.kt:15)
    at kotlin.coroutines.CombinedContext.get(CoroutineContextImpl.kt:120)
    at arrow.fx.coroutines.CancelBoundaryKt.cancelBoundary(cancelBoundary.kt:37)
    at arrow.fx.coroutines.DebugProbesTest$example$1$1$3.invokeSuspend(DebugProbesTest.kt:180)
    (Coroutine creation stacktrace)
    at kotlin.coroutines.intrinsics.IntrinsicsKt__IntrinsicsJvmKt.createCoroutineUnintercepted(IntrinsicsJvm.kt:87)
    at arrow.fx.coroutines.StartCoroutineCancellableKt.startCoroutineCancellable(startCoroutineCancellable.kt:33)
    at arrow.fx.coroutines.DebugProbesTest$example$1$1.invokeSuspend(DebugProbesTest.kt:174)
    at kotlin.coroutines.jvm.internal.BaseContinuationImpl.resumeWith(ContinuationImpl.kt:33)
    at arrow.fx.coroutines.ExecutorServiceContinuation$resumeWith$1.run(dispatchers.kt:62)
    at java.util.concurrent.ForkJoinTask$RunnableExecuteAction.exec(ForkJoinTask.java:1402)
    at java.util.concurrent.ForkJoinTask.doExec(ForkJoinTask.java:289)
    at java.util.concurrent.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1056)
    at java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1692)
```