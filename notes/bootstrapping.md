When there is no resume token saved, we'd like to "start at the beginning."

Whatever that means.

Can mean two things:

1. Start from oldest entry in oplog
2. Start from the beginning of time

2 can sort of be done if we treat all current records as inserts and publish those (which is what 
the mongodb kafka connector source task does).

1 can be done by looking at the oplog itself, which apparently is not so hard. As seen by 
getReplicationInfo shell command javascript snippet:

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

