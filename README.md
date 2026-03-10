# FreeSeq

为了在前端用时序图展示后端多线程任务的进度，FreeSeq 用于在后端向前端实时传输线程 fork join 时间。

## Concepts

-   每一个 `Thread` 代表前端时序图中的每一条生命线。
-   每一个 `Worker` 代表后端的一个协程。

每个时刻，每个 `Worker` 在且仅在一个 `Thread` 上工作。

-   `freeseq.fork` 用于将当前 `Worker` 所在的 `Thread` 分叉出一个新的 `Thread`。

    这在前端时序图中表现为一条新的生命线以及一条从当前生命线指向新生命线的消息线。

-   `freeseq.join` 用于将另一个 `Thread` 聚合到当前 `Worker` 所在的 `Thread` 上。

    这在前端时序图中表现为另一条生命线从此刻开始进入静息状态以及一条从它指向当前生命线的消息线。

-   `Worker.exec` 使当前 `Worker` 离开所在 `Thread` 前往另一个 `Thread` 去工作。

    这在前端时序图中表现为从此刻开始当前生命线进入静息状态，另一条生命线从此刻开始进入活跃状态。
