package org.tikv.cdc;

public interface ICDCClientV2 {
    void execute(final long startTs);

    long getResolvedTs();

    /** @return null if no more data */
    RawKVEntry get();

    void close();
}
