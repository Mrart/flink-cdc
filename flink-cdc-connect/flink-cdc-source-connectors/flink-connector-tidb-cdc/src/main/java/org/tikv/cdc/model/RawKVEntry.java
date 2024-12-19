package org.tikv.cdc.model;

import org.tikv.shade.com.google.protobuf.ByteString;

public class RawKVEntry {
    private final OpType opType;
    private final ByteString key;
    private ByteString value;
    private ByteString oldValue;
    private final long startTs;
    private final long crts;
    private final long regionId;

    public RawKVEntry(RawKVEntry raw) {
        this.opType = raw.getOpType();
        this.key = raw.getKey();
        this.value = raw.getValue();
        this.oldValue = raw.getOldValue();
        this.startTs = raw.getStartTs();
        this.crts = raw.getCrts();
        this.regionId = raw.getRegionId();
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

    public boolean isUpdate() {
        return this.opType.equals(OpType.Put) && this.oldValue == null && this.value != null;
    }

    public RawKVEntry[] splitUpdateKVEntry(RawKVEntry raw) {
        if (raw == null) {
            throw new RuntimeException("Null raw kv entry cannot be split.");
        }
        // Create the delete and insert entries by copying the raw entry
        RawKVEntry deleteKVEntry = new RawKVEntry(raw);
        deleteKVEntry.setValue(null); // Set value to null for delete entry

        RawKVEntry insertKVEntry = new RawKVEntry(raw);
        insertKVEntry.setOldValue(null); // Set oldValue to null for insert entry

        return new RawKVEntry[] {deleteKVEntry, insertKVEntry};
    }
}
