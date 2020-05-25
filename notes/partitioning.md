Without partitioning there is no load balancing of changes; effectively all changes are totally 
ordered via single thread.

Currently, the collection is a natural partition. I suppose someone may want to order an entire
database, which I believe is possible with change streams, so we should support it.

Partitioning via a query is easy, but it assumes you know the partitions ahead of time. Could we 
dynamically lock partitions as they were discovered?

1. change heard (first, multiple threads would have to get this before they partitioned themselves)
2. extract partition key (using some user provided f(change) -> key)
3. lock partition
4. process
5. commit

Compared to current impl:

* Change is listened to without lock (but still needs resume token?/offset)
* Lock is attempted as a function of a change, after a change, not before

One downside with the above algorithm is that each thread will see all changes, and only one will
do any productive work.

Another option might be to still do a preemptive lock, but query for the partition keys that we can 
lock on.

d1 -> k=1
d2 -> k=2

query distinct k -> k=1, k=2
n1: lock on k=1
n2: lock on k=2

Static and dynamic partitions could then be built using the same abstraction: some locks a subset of
records. The query for the subset can be static or dynamic.

Updating the dynamic query could be done by polling. We could also listen to changes to get new
partitions. This might get complex... or not? Two options I can think of:

1. Filter the actual process change streams ahead of time based on locked partitions, and have an
additional superset change stream which notifies us of new partitions.

What defines a partition? 

A query with specific values. But also a pipeline (or stage...?) to get those specific values. Which
should then be translatable to queries for each of those values.

Load balancing partitions

If there are a 10 partitions, and 2 nodes, both nodes should get 5 partitions. But what if nodes are
added?

What if there are millions of partitions? Would we really have a million queries? Probably need the
idea of partition ranges in that case. Or some kind of modulus or hashing. See cassandra node 
balancing. The lock-per-change works better in that scenario I think. It just rolls with it 
regardless of the number of partitions.


