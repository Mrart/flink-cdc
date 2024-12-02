package org.tikv.cdc;

import org.tikv.kvproto.Cdcpb;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

public class RawKVEntry {
    private OpType opType;
    private ByteString key;
    private ByteString value;
    private ByteString oldValue;
    private long startTs;
    private long crts;
    private long regionId;

    public boolean isUpdate() {
        return this.opType.equals(OpType.OpTypePut) && this.oldValue == null && this.value != null;
    }

    private RawKVEntry(Builder builder) {
        this.opType = builder.opType;
        this.key = builder.key;
        this.value = builder.value;
        this.oldValue = builder.oldValue;
        this.startTs = builder.startTs;
        this.crts = builder.crts;
        this.regionId = builder.regionId;
    }

    public OpType getOpType() {
        return opType;
    }

    public ByteString getKey() {
        return key;
    }

    public ByteString getValue() {
        return value;
    }

    public ByteString getOldValue() {
        return oldValue;
    }

    public long getStartTs() {
        return startTs;
    }

    public long getCrts() {
        return crts;
    }

    public long getRegionId() {
        return regionId;
    }

    public static class Builder {
        private OpType opType;
        private ByteString key;
        private ByteString value;
        private ByteString oldValue;
        private long startTs;
        private long crts;
        private long regionId;

        public Builder setOpType(OpType opType) {
            this.opType = opType;
            return this;
        }

        public Builder setKey(ByteString key) {
            this.key = key;
            return this;
        }

        public Builder setValue(ByteString value) {
            this.value = value;
            return this;
        }

        public Builder setOldValue(ByteString oldValue) {
            this.oldValue = oldValue;
            return this;
        }

        public Builder setStartTs(long startTs) {
            this.startTs = startTs;
            return this;
        }

        public Builder setCrts(long crts) {
            this.crts = crts;
            return this;
        }

        public Builder setRegionId(long regionId) {
            this.regionId = regionId;
            return this;
        }

        public RawKVEntry build() {
            // You can add validation logic here if needed
            return new RawKVEntry(this);
        }
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

    public RegionFeedEvent assembleRowEvent(long regionId, Cdcpb.Event.Row row) {
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
