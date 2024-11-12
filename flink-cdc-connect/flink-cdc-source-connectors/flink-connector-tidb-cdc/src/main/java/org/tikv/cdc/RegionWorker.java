package org.tikv.cdc;

import org.tikv.kvproto.Cdcpb.Event.Row;

/**
 * A Region worker is responsible for all regions in a TiKV store. The Region worker reads the grpc
 * response from its input chan, processes it, and writes it to Puller's eventChan.
 */
public class RegionWorker {

  public RegionFeedEvent assembleRowEvent(long regionId, Row row) {
    RawKVEntry rawKVEntry =
        new RawKVEntry.Builder()
            .setOpType(OpType.valueOf(row.getOpType().getNumber()))
            .setRegionId(regionId)
            .setKey(row.getKey())
            .setValue(row.getValue())
            .setStartTs(row.getStartTs())
            .setCrts(row.getCommitTs())
            .setOldValue(row.getOldValue())
            .build();
    RegionFeedEvent reEvent = new RegionFeedEvent();
    reEvent.setRegionId(regionId);
    reEvent.setRawKVEntry(rawKVEntry);
    return reEvent;
  }
}
