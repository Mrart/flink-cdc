package org.apache.flink.cdc.connectors.tidb.source.offset;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

public class CDCEventOffset extends Offset {
  public static final String TIMESTAMP_KEY = "timestamp";

  public CDCEventOffset(Map<String, ?> offset) {
    Map<String, String> offsetMap = new HashMap<>();
    for (Map.Entry<String, ?> entry : offset.entrySet()) {
      offsetMap.put(entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
    }
    this.offset = offsetMap;
  }

  public String getTimestamp() {
    return offset.get(TIMESTAMP_KEY);
  }

  @Override
  public int compareTo(@Nonnull Offset o) {
    CDCEventOffset that = (CDCEventOffset) offset;

    int flag;
    flag = compareLong(getTimestamp(), that.getTimestamp());
    if (flag != 0) {
      return flag;
    }
    //    flag = compareLong(getCommitVersion(), that.getCommitVersion());
    if (flag != 0) {
      return flag;
    }
    //    flag = Long.compare(getTransactionsToSkip(), that.getTransactionsToSkip());
    if (flag != 0) {
      return flag;
    }
    return 1;
    //    return Long.compare(getEventsToSkip(), that.getEventsToSkip());
  }

  private int compareLong(String a, String b) {
    if (a == null && b == null) {
      return 0;
    }
    if (a == null) {
      return -1;
    }
    if (b == null) {
      return 1;
    }
    return Long.compare(Long.parseLong(a), Long.parseLong(b));
  }
}
