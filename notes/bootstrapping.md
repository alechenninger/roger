When there is no resume token saved, we'd like to "start at the beginning."

Whatever that means.

Can mean two things:

1. Start from oldest entry in oplog
2. Start from the beginning of time

2 can sort of be done if we treat all current records as inserts and publish those (which is what 
the mongodb kafka connector source task does).

Where do we pick up from the change stream though? There is no way really unless each document 
contains a corresponding resume token (or timestamp? if logical timestamp), and the token is 
sortable, so we can know which change was not already observed.

I suppose if you work from a static backup that is not accepting writes, you can use the existing
documents to replay "changes", and the existing oplog to know when to pick back up, by timestamp.
The timestamp isn't as reliable as a resume token though, I imagine.

Either way this throws out the order relationships between documents themselves.

We can consider integrating our application such that each process does not accept writes until its
listener has started (that is, ensure our listener is started before "the beginning of time"). 
However, there is a race condition here: 

| time | operation |
|------|-----------|
| t1   | listener started |
| t2   | inserted record. |
| t3   | listener crashes. |
| t4   | restarted listener does not see record at t2. |

Granted, perhaps it is unlikely enough.

You can also consider a manual bootstrapping that inserts a dummy record(s) until a listener 
successfully sees and saves the associated resume token with that change.

Let's try to do better: option 1.

Starting the change stream in the past without a resume token requires a timestamp, and that 
timestamp must be reachable within the oplog. This means we have to look at the oplog itself, 
which apparently is not so hard, as seen by getReplicationInfo shell 
command javascript snippet:

```javascript
function () {
        var localdb = this.getSiblingDB("local");

        var result = {};
        var oplog;
        var localCollections = localdb.getCollectionNames();
        if (localCollections.indexOf('oplog.rs') >= 0) {
            oplog = 'oplog.rs';
        } else {
            result.errmsg = "replication not detected";
            return result;
        }

        var ol = localdb.getCollection(oplog);

        var firstc = ol.find().sort({$natural: 1}).limit(1);
        var lastc = ol.find().sort({$natural: -1}).limit(1);
        if (!firstc.hasNext() || !lastc.hasNext()) {
            result.errmsg =
                "objects not found in local.oplog.$main -- is this a new and empty db instance?";
            result.oplogMainRowCount = ol.count();
            return result;
        }

        var first = firstc.next();
        var last = lastc.next();
        var tfirst = first.ts;
        var tlast = last.ts;
```

Of course, it is limited by oplog size. But, the use case I am thinking of would be publishing 
changes from the beginning, so this bootstrapping problem is really only a problem when the oplog is
small or even empty. Effectively the listener is like another replica set member: it must be 
listening to the oplog before it fills up or it needs to be resync'd. Except in this case, there is
no resync solution I'm aware of (perhaps we could reverse engineer what replica sets do in that 
case). 

However, this also may be brittle; I'm not sure if the oplog schema is guaranteed to be stable. They
created change streams so we could stop looking at the oplog after all ^_^. That said it is easy to 
test for should it break.

A naive approach still has a race condition, a possible phantom read:

| time | operation |
|------|-----------|
| t1   | check for op log entry. none. |
| t2   | inserted record. |
| t3   | start change stream without resume token or timestamp. |

This scenario misses the t2 insert change.

We can solve this by never starting the listener until there is an oplog entry.
