package org.tikv.cdc.kv;

import org.tikv.common.meta.TiTableInfo;

import java.util.Optional;

public interface ICDCClientV2 {
    void start(final long startTs);

    long getResolvedTs();

    Optional<TiTableInfo> getTableInfo(String database, String name);

    public void addListener(EventListener listener);

    public boolean isRunning();

    void close();
}
