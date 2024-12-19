package org.tikv.cdc.model;

import org.tikv.kvproto.Coprocessor;

public class RegionKeyRange {
    private long regionId;
    private Coprocessor.KeyRange keyRange;

    public RegionKeyRange(long regionId, Coprocessor.KeyRange keyRange) {
        this.regionId = regionId;
        this.keyRange = keyRange;
    }

    public long getRegionId() {
        return regionId;
    }

    public void setRegionId(long regionId) {
        this.regionId = regionId;
    }

    public Coprocessor.KeyRange getKeyRange() {
        return keyRange;
    }

    public void setKeyRange(Coprocessor.KeyRange keyRange) {
        this.keyRange = keyRange;
    }
}
