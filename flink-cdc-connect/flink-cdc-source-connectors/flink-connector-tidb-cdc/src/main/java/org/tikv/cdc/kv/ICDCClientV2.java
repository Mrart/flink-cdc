package org.tikv.cdc.kv;

import org.tikv.cdc.model.RegionFeedEvent;
import org.tikv.common.meta.TiTableInfo;

public interface ICDCClientV2 {
    void execute(final long startTs);

    long getResolvedTs();

    TiTableInfo getTableInfo(String database, String name);

    /** @return null if no more data */
    RegionFeedEvent get();

    void close();
}
