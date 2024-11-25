package org.tikv.cdc;

import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

public class CDCClientV2Impl implements CDCClientV2 {
  private final TiConfiguration tiConf;
  private final TiSession tiSession;
  private final StreamSplit split;

  public CDCClientV2Impl(TiConfiguration tiConf, StreamSplit split) {
    this.tiConf = tiConf;
    this.split = split;
    this.tiSession = new TiSession(tiConf);
  }

  @Override
  public void start() {}

  @Override
  public long getResolvedTs() {
    return 0;
  }

  @Override
  public RawKVEntry get() {
    return null;
  }

  @Override
  public void close() {}
}
