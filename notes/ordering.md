Processing in order requires use of a fencing token (I believe Martin Kleppmann coined the term), as
a lock holder cannot detect it has lost the lock before or during processing a change; side effects
must participate in the lock by tracking and comparing the fencing token.

The token can be a version number inside the changed documents themselves or we can use a version
associated with the lock. The choice of which to use depends on the system. For example, if the
change stream is the only trigger of some side effects, then the lock version can be used. If other
operations may also trigger similar side effects or work with overlapping resources, then those too
may need a fencing token, and so the change callback may need to use this instead.

Write concern and fencing tokens

1. If the fencing token comes from documents themselves, we do not need our commit operation to be
durable. That said it changes the semantics a bit: if we our out of order, we may want to keep going
instead of stopping because we think we lost the lock. We could no longer assume old fencing token
means old lock, so we'd have to try and commit anyway, with acknowledged write concern (so we can
still see if we lost the lock, then).
2. If the fencing token comes from the lock version, we do need the commit operation to be durable,
because the resume token prevents out-of-order processing (the next lock will have a higher version,
but could process an earlier change)
3. If the fencing token comes from the lock version, but the lock version is incremented with each
change on commit, then we do not need our commit operation to be durable. The next lock will have a
lower version, and changes will be no-ops until they catch back up. Like 1, detecting an old fencing
token does not mean the lock is lost, we need to try committing. 

We could potentially support these variations using well documented consumer and lock strategies.

1. Lock strategy does not generate version, ack concern commit, change consumer must use own tokens,
and should not throw a LostLockException if change is outdated.
2. This is how it works right now. The consumer MAY throw a lost lock exception but it technically
doesn't have to.
3. Lock strategy generates version per ack'd commit.
