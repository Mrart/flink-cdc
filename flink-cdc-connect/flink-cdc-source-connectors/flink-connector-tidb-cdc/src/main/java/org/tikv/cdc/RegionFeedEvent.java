package org.tikv.cdc;

import org.tikv.kvproto.Coprocessor;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

public class RegionFeedEvent {

  private long regionId;

  private RawKVEntry rawKVEntry;

  private ResolvedKeyRanges resolved;

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

  public static class RawKVEntry {
    private OpType opType;
    private ByteString key;
    private ByteString value;
    private ByteString oldValue;
    private long startTs;
    private long crts;
    private long regionId;

    public OpType getOpType() {
      return opType;
    }

    public void setOpType(OpType opType) {
      this.opType = opType;
    }

    public ByteString getKey() {
      return key;
    }

    public void setKey(ByteString key) {
      this.key = key;
    }

    public ByteString getValue() {
      return value;
    }

    public void setValue(ByteString value) {
      this.value = value;
    }

    public ByteString getOldValue() {
      return oldValue;
    }

    public void setOldValue(ByteString oldValue) {
      this.oldValue = oldValue;
    }

    public long getStartTs() {
      return startTs;
    }

    public void setStartTs(long startTs) {
      this.startTs = startTs;
    }

    public long getCrts() {
      return crts;
    }

    public void setCrts(long crts) {
      this.crts = crts;
    }

    public long getRegionId() {
      return regionId;
    }

    public void setRegionId(long regionId) {
      this.regionId = regionId;
    }

    public boolean isUpdate() {
      return this.opType.equals(OpType.OpTypePut) && this.oldValue == null && this.value != null;
    }

    public List<RawKVEntry> splitUpdateKVEntry(RawKVEntry rawKVEntry) {
      List<RawKVEntry> rawKVEntries = new ArrayList<>();
      if (rawKVEntry == null) {
        throw new RuntimeException("null raw kv entry cannot be split.");
      }
      RawKVEntry deleteKVEntry = rawKVEntry;
      deleteKVEntry.value = null;
      RawKVEntry insertKVEntry = rawKVEntry;
      insertKVEntry.oldValue = null;
      rawKVEntries.add(deleteKVEntry);
      rawKVEntries.add(insertKVEntry);
      return rawKVEntries;
    }
  }

  public static class ResolvedKeyRanges {
    private List<Coprocessor.KeyRange> keyRanges;
    private long resolvedTs;

    public List<Coprocessor.KeyRange> getKeyRanges() {
      return keyRanges;
    }

    public void setKeyRanges(List<Coprocessor.KeyRange> keyRanges) {
      this.keyRanges = keyRanges;
    }

    public long getResolvedTs() {
      return resolvedTs;
    }

    public void setResolvedTs(long resolvedTs) {
      this.resolvedTs = resolvedTs;
    }
  }
}
