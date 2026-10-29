# LiveRecall Networking Primer

This is a short bridge from earlier socket/TCP work to the mechanics used in Assignment 4. It is **not** a solution design.

## WebSocket and TCP

A WebSocket connection provides application messages over a TCP connection. TCP remains reliable and ordered. Linux `tc/netem` packet loss or reordering can therefore increase latency through retransmission and head-of-line blocking even though the application normally still receives an ordered WebSocket byte stream.

## `asyncio`

`asyncio` lets one Python process manage many connections without one OS thread per client. An `async` function may pause at `await`, allowing other tasks to run.

## Producer and consumer

The caption source is a producer. Each student connection is a consumer. If captions are generated faster than one client can consume them, pending work can accumulate.

## Per-client queue

A common server architecture places pending outbound work for each client in a separate queue. A slow client's queue can then be managed independently from normal clients. The assignment does **not** require a particular queue implementation or queue size.

## Backpressure

Backpressure means the receiver or network cannot currently consume data as fast as it is produced. A robust real-time application needs a deliberate policy for this condition rather than allowing memory and delay to grow indefinitely.

## Bounded state

"Bounded" means there is a finite policy: history, queues, or other retained state cannot grow forever as a lecture continues or a client remains slow. Teams choose and justify their bounds.

## Semantic coalescing

Revisions have meaning. If revision 1 and revision 2 for the same unfinished segment are both waiting and revision 3 becomes available, some older provisional state may no longer be useful. The assignment permits suppression of obsolete provisional revisions under genuine overload, while final captions remain durable state.

The design of **when**, **where**, and **how** to apply such a policy is part of the graded work.
