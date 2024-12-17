package org.tikv.cdc;

import org.tikv.common.meta.TiTableInfo;

public interface ICDCClientV2 {
    void execute(final long startTs, final String dbName, final String tableName);

    long getResolvedTs();

    TiTableInfo getTableInfo(String database, String name);

    /** @return null if no more data */
    RegionFeedEvent get();

    void close();
}
