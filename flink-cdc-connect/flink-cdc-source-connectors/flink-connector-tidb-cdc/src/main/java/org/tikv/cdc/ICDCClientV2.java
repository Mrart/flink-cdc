package org.tikv.cdc;

import io.debezium.relational.TableId;
import org.tikv.common.meta.TiTableInfo;

public interface ICDCClientV2 {
  void execute(final long startTs, final TableId tableId);

  long getResolvedTs();

  TiTableInfo getTableInfo(String database, String name);

  /** @return null if no more data */
  RegionFeedEvent get();

  void close();
}
