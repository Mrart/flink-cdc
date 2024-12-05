package org.tikv.cdc;

public interface ICDCClientV2 {
  void execute(final long startTs, final long tableId);

  long getResolvedTs();

  /** @return null if no more data */
  RegionFeedEvent get();

  void close();
}
