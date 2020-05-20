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
