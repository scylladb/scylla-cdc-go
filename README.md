# scylla-cdc-go

A simple library for reading from CDC log.
Refer to the example for information on how to use it.

DISCLAIMER: This library is a work in progress, and many features are still missing. This includes:

- Cluster topology change support (adding/removing nodes from the cluster)
- Rate limiting (it's hitting the cluster pretty hard right now)
- Distributing work over multiple machines

## How it works (in a nutshell)

When a write operation is performed on the base table with CDC enabled, one or more entries describing the change are appended to the CDC log table.

This library actively polls for those entries and processes them using a `ChangeConsumer`, which is an object provided by the user. The algorithm, in a simplified form, looks like this:

```
LastReadTimestamp <- current time
repeat until stopped or error occurs:
    ReadRangeStart <- LastReadTimestamp - LowerBoundReadOffset
    ReadRangeEnd <- current time - UpperBoundReadOffset
    Changes <- SELECT changes with timestamps between ReadRangeStart and ReadRangeEnd
    for each Change in Changes:
        ChangeConsumer.Consume(Change)
    LastReadTimestamp <- max of timestamps of processed Changes
```

The algorithm above is simplified. In reality, it is not guaranteed that changes from multiple partitions will be processed in their timestamp order.

## How to configure it

There are two main knobs in `ReaderConfig`: `LowerBoundReadOffset`, and `UpperBoundReadOffset`. The choice of parameters depends on whether you need to process changes quickly as they appear, or you are OK with processing them with some delay.

1. If you care about processing changes quickly, there is an issue that you need to know about - CDC log rows are not guaranteed to appear in the log in their timestamp order. This means that after you poll and process changes up to timestamp T, a change with a timestamp slightly lower than T may appear in CDC log. Because of this, you need to be prepared to extend your polling window slightly into the past, be ready to deduplicate entries and accept occasional reordering when processing changes.
   1. `LowerBoundReadOffset` needs to be set to a positive value, so that you can check for re-ordered entries that could appear later
   2. `UpperBoundReadOffset` needs to be set to zero, or a negative value - in order to process recent changes.
2. If you do not care about processing changes quickly, you can deliberately delay processing of entries by a fixed amount of time. Reordering of changes usually isn't very extreme, and usually a fixed time interval can be specified which prevents such issues. In such case:
   1. `LowerBoundReadOffset` can be set to 0 - this way you won't be processing the same change twice,
   2. `UpperBoundReadOffset` needs to be set to a positive value - this way you process new changes after a delay, when you are confident that the reordering won't happen.
