package org.tikv.cdc;

import org.tikv.kvproto.Cdcpb;

import java.util.List;

public class RegionFeedEvent {

  //  public enum Type {
  //    DDL,PerWrite,Commit;
  //  }

  private long regionId;

  private RawKVEntry rawKVEntry;

  private ResolvedKeyRanges resolved;

  private String dbName;
  private String tableName;

  public long getRegionId() {
    return regionId;
  }

  public void setRegionId(long regionId) {
    this.regionId = regionId;
  }

  public RawKVEntry getRawKVEntry() {
    return rawKVEntry;
  }

  public void setRawKVEntry(RawKVEntry rawKVEntry) {
    this.rawKVEntry = rawKVEntry;
  }

  public ResolvedKeyRanges getResolved() {
    return resolved;
  }

  public void setResolved(ResolvedKeyRanges resolved) {
    this.resolved = resolved;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public static class ResolvedKeyRanges {
    private List<RegionKeyRange> keyRanges;
    private long resolvedTs;

    public List<RegionKeyRange> getKeyRanges() {
      return keyRanges;
    }

    public void setKeyRanges(List<RegionKeyRange> keyRanges) {
      this.keyRanges = keyRanges;
    }

    public long getResolvedTs() {
      return resolvedTs;
    }

    public void setResolvedTs(long resolvedTs) {
      this.resolvedTs = resolvedTs;
    }
  }

  public static RegionFeedEvent assembleRowEvent(long regionId, Cdcpb.Event.Row row) {
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
