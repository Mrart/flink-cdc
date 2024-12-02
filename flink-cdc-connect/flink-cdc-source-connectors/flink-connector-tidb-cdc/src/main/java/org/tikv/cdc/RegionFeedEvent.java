package org.tikv.cdc;

import org.tikv.kvproto.Coprocessor;

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
