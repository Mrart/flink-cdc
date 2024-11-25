package org.tikv.cdc;

public interface CDCClientV2 {
  void start();

  long getResolvedTs();

  /** @return null if no more data */
  RawKVEntry get();

  void close();
}
