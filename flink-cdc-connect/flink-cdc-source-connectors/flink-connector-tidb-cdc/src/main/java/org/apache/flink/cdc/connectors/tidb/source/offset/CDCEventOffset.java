package org.apache.flink.cdc.connectors.tidb.source.offset;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.cdc.connectors.tidb.table.utils.TSOUtils.TimestampToTSO;

public class CDCEventOffset extends Offset {
  public static final String TIMESTAMP_KEY = "timestamp";
  // TimeStamp Oracle from pd
  public static final String COMMIT_VERSION_KEY = "commit_version";

  public static final CDCEventOffset INITIAL_OFFSET =
      new CDCEventOffset(Collections.singletonMap(TIMESTAMP_KEY, "0"));
  public static final CDCEventOffset NO_STOPPING_OFFSET = new CDCEventOffset(Long.MAX_VALUE);

  public CDCEventOffset(Map<String, ?> offset) {
    Map<String, String> offsetMap = new HashMap<>();
    for (Map.Entry<String, ?> entry : offset.entrySet()) {
      offsetMap.put(entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
    }
    this.offset = offsetMap;
  }

  public CDCEventOffset(@Nonnull String timestamp, String commitVersion) {
    Map<String, String> offsetMap = new HashMap<>();
    offsetMap.put(TIMESTAMP_KEY, timestamp);
    if (commitVersion != null) {
      offsetMap.put(COMMIT_VERSION_KEY, commitVersion);
    }
    this.offset = offsetMap;
  }

  public CDCEventOffset(long binlogEpochMill) {
    Map<String, String> offsetMap = new HashMap<>();
    offsetMap.put(TIMESTAMP_KEY, String.valueOf(binlogEpochMill));
    offsetMap.put(COMMIT_VERSION_KEY, String.valueOf(TimestampToTSO(binlogEpochMill)));
    this.offset = offsetMap;
  }

  public String getTimestamp() {
    return offset.get(TIMESTAMP_KEY);
  }

  public String getCommitVersion() {
    if (offset.get(COMMIT_VERSION_KEY) == null) {
      String timestamp = getTimestamp();
      // timestamp to commit version.
      return String.valueOf(TimestampToTSO(Long.parseLong(timestamp)));
    }
    return offset.get(COMMIT_VERSION_KEY);
  }

  @Override
  public int compareTo(@Nonnull Offset o) {
    CDCEventOffset that = (CDCEventOffset) o;

    int flag;
    flag = compareLong(getTimestamp(), that.getTimestamp());
    if (flag != 0) {
      return flag;
    }
    return compareLong(getCommitVersion(), that.getCommitVersion());
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

  public static CDCEventOffset of(Map<String, ?> offsetMap) {
    Map<String, String> offsetStrMap = new HashMap<>();
    for (Map.Entry<String, ?> entry : offsetMap.entrySet()) {
      offsetStrMap.put(
          entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
    }
    return new CDCEventOffset(offsetStrMap);
  }

  public static long getStartTs(Offset offset) {
    if (offset.getOffset().get(COMMIT_VERSION_KEY) != null) {
      return Long.parseLong(offset.getOffset().get(COMMIT_VERSION_KEY));
    } else {
      return TimestampToTSO(Long.parseLong(offset.getOffset().get(TIMESTAMP_KEY)));
    }
  }
}
