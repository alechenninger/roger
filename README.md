# roger

> Inspired by psychologist [Carl Rogers](https://en.wikipedia.org/wiki/Carl_Rogers) who coined the 
> term Active Listening. Listening is good, and this library listens. 
>
> Also: ["Roger that!"](https://en.wiktionary.org/wiki/roger_that)

MongoDB change stream listener with message-queue-like (competing consumer) semantics.

Normally, listening to a change stream is more like a topic–that is, each listener receives the 
change. Additionally, there is no built-in tracking of where you left off. If you...

* Want your listener to be HA/redundant across multiple processes
* Do not want to process each change redundantly for each process
* Want to process changes in order

...then what you want is a queue!

> **NOTE:** This library implements _at least once_ guaranteed delivery, which means it is still 
> possible for a change to be "heard" and processed more than once.

To do this we use a locking algorithm that coordinates multiple listeners.

For more information, see [this blog post](https://www.alechenninger.com/2020/05/building-kafka-like-message-queue-with.html).
