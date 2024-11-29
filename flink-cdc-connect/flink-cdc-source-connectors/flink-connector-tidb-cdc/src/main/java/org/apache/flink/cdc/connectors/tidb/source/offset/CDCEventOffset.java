package org.apache.flink.cdc.connectors.tidb.source.offset;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CDCEventOffset extends Offset {
  public static final String TIMESTAMP_KEY = "timestamp";
  public static final String EVENTS_TO_SKIP_KEY = "events";

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

  public CDCEventOffset(long timestamp) {
    this(Long.toString(timestamp), 0);
  }

  public CDCEventOffset(@Nonnull String timestamp, long eventsToSkip) {
    Map<String, String> offsetMap = new HashMap<>();
    offsetMap.put(TIMESTAMP_KEY, timestamp);
    offsetMap.put(EVENTS_TO_SKIP_KEY, String.valueOf(eventsToSkip));
    this.offset = offsetMap;
  }

  public CDCEventOffset(long restartSkipEvents, long binlogEpochSecs) {
    Map<String, String> offsetMap = new HashMap<>();
    offsetMap.put(EVENTS_TO_SKIP_KEY, String.valueOf(restartSkipEvents));
    offsetMap.put(TIMESTAMP_KEY, String.valueOf(binlogEpochSecs));
    this.offset = offsetMap;
  }

  public String getTimestamp() {
    return offset.get(TIMESTAMP_KEY);
  }

  @Override
  public int compareTo(@Nonnull Offset o) {
    CDCEventOffset that = (CDCEventOffset) o;

    int flag;
    flag = compareLong(getTimestamp(), that.getTimestamp());
    if (flag != 0) {
      return flag;
    }
    return Long.compare(getEventsToSkip(), that.getEventsToSkip());
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

  public static CDCEventOffset of(Map<String,?> offsetMap){
    Map<String,String> offsetStrMap = new HashMap<>();
    for (Map.Entry<String,?> entry: offsetMap.entrySet()){
      offsetStrMap.put(
              entry.getKey(),entry.getValue() == null ? null : entry.getValue().toString());
    }
    return new CDCEventOffset(offsetStrMap);
  }

  public long getEventsToSkip() {
    return longOffsetValue(offset, EVENTS_TO_SKIP_KEY);
  }
}
