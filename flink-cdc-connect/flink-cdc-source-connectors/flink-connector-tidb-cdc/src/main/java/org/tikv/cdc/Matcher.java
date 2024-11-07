package org.tikv.cdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Cdcpb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Matcher {
  private static final Logger logger = LoggerFactory.getLogger(Matcher.class);

  private Map<MatchKey, Cdcpb.Event.Row> unmatchedValue; // 缓存未匹配的预写事件
  private List<Cdcpb.Event.Row> cachedCommit; // 缓存提交事件
  private List<Cdcpb.Event.Row> cachedRollback; // 缓存回滚事件

  public Matcher() {
    this.unmatchedValue = new HashMap<>();
    this.cachedCommit = new ArrayList<>();
    this.cachedRollback = new ArrayList<>();
  }

  // 将预写事件放入 unmatchedValue 缓存中
  public void putPrewriteRow(Cdcpb.Event.Row row) {
    MatchKey key = new MatchKey(row.getStartTs(), row.getKey().toString());
    // 跳过 fake prewrite 事件
    if (unmatchedValue.containsKey(key) && row.getValue().isEmpty()) {
      return;
    }
    unmatchedValue.put(key, row);
  }

  // 匹配提交事件和预写事件
  public boolean matchRow(Cdcpb.Event.Row row, boolean initialized) {
    MatchKey key = new MatchKey(row.getStartTs(), String.valueOf(row.getKey()));
    Cdcpb.Event.Row value = unmatchedValue.get(key);
    if (value != null) {
      // TiKV 可能发送空值的 fake prewrite 事件
      if (!initialized && value.getValue().isEmpty()) {
        return false;
      }
      //      row.setValue(value.getValue());
      //      row.setOldValue(value.getOldValue());
      unmatchedValue.remove(key);
      return true;
    }
    return false;
  }

  // 缓存提交事件
  public void cacheCommitRow(Cdcpb.Event.Row row) {
    cachedCommit.add(row);
  }

  // 匹配缓存的提交事件
  public List<Cdcpb.Event.Row> matchCachedRow(boolean initialized) {
    if (!initialized) {
      throw new IllegalStateException("Must be initialized before matching cached rows");
    }

    List<Cdcpb.Event.Row> matchedCommit = new ArrayList<>();
    for (Cdcpb.Event.Row cacheEntry : cachedCommit) {
      boolean matched = matchRow(cacheEntry, true);
      if (!matched) {
        logger.info(
            "Ignore commit event without prewrite, key: {}, startTs: {}",
            cacheEntry.getKey(),
            cacheEntry.getStartTs());
        continue;
      }
      matchedCommit.add(cacheEntry);
    }
    return matchedCommit;
  }

  // 回滚事件处理
  public void rollbackRow(Cdcpb.Event.Row row) {
    MatchKey key = new MatchKey(row.getStartTs(), String.valueOf(row.getKey()));
    unmatchedValue.remove(key);
  }

  // 缓存回滚事件
  public void cacheRollbackRow(Cdcpb.Event.Row row) {
    cachedRollback.add(row);
  }

  // 处理缓存的回滚事件
  public void matchCachedRollbackRow(boolean initialized) {
    if (!initialized) {
      throw new IllegalStateException("Must be initialized before matching cached rollback rows");
    }

    for (Cdcpb.Event.Row cacheEntry : cachedRollback) {
      rollbackRow(cacheEntry);
    }
  }
}
