package org.tikv.cdc;

/** Listening to log interface */
public interface EntryListener {
    /** send */
    void notify(RawKVEntry entry);

    void resolvedTs(long resolvedTs);

    void close();
}
