package org.tikv.cdc.frontier;

@FunctionalInterface
public interface EntryConsumer {
    boolean accept(SkipListNode node);
}
