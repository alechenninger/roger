Processing in order requires use of a fencing token (I believe Martin Kleppmann coined the term), as
a lock holder cannot detect it has lost the lock before or during processing a change; side effects
must participate in the lock by tracking and comparing the fencing token.

The token can be a version number inside the changed documents themselves or we can use a version
associated with the lock. The choice of which to use depends on the system. For example, if the
change stream is the only trigger of some side effects, then the lock version can be used. If other
operations may also trigger similar side effects or work with overlapping resources, then those too
may need a fencing token, and so the change callback may need to use this instead.
