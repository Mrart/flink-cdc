package org.tikv.cdc;

import org.tikv.kvproto.Cdcpb.Event.Row;

/**
 * A Region worker is responsible for all regions in a TiKV store. The Region worker reads the grpc
 * response from its input chan, processes it, and writes it to Puller's eventChan.
 */
public class RegionWorker {

  public RegionFeedEvent assembleRowEvent(long regionId, Row row) {
    RegionFeedEvent.RawKVEntry rawKVEntry = new RegionFeedEvent.RawKVEntry();
    rawKVEntry.setOpType(OpType.valueOf(row.getOpType().getNumber()));
    rawKVEntry.setRegionId(regionId);
    rawKVEntry.setKey(row.getKey());
    rawKVEntry.setValue(row.getValue());
    rawKVEntry.setStartTs(row.getStartTs());
    rawKVEntry.setCrts(row.getCommitTs());
    rawKVEntry.setOldValue(row.getOldValue());
    RegionFeedEvent reEvent = new RegionFeedEvent();
    reEvent.setRegionId(regionId);
    reEvent.setRawKVEntry(rawKVEntry);
    return reEvent;
  }
}
