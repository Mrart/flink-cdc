package org.tikv.cdc.frontier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.util.BytesUtils;
import org.tikv.kvproto.Coprocessor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KeyRangeFrontier implements Frontier {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyRangeFrontier.class);
  public static long FAKE_REGION_ID = 0L;
  private final SkipList spanList;
  private final FibonacciHeap minTsHeap;
  //  private SkipListNode[] seekTempResult;
  private final Map<Long, SkipListNode> cachedRegions;
  //  private final Metrics metrics; // Assuming Metrics is a custom class to track metrics

  public KeyRangeFrontier(long checkpointTs, Coprocessor.KeyRange... spans) {
    this.spanList = new SkipList();
    this.minTsHeap = new FibonacciHeap();
    this.cachedRegions = new HashMap<>();
    // high
    //    this.seekTempResult = new SkipListNode[MAX_HEIGHT];

    // Initialize frontier with spans
    boolean firstRange = true;
    for (Coprocessor.KeyRange span : spans) {
      if (firstRange) {
        spanList.insert(span.getStart().toByteArray(), minTsHeap.insert(checkpointTs));
        spanList.insert(span.getEnd().toByteArray(), minTsHeap.insert(Long.MAX_VALUE));
        firstRange = false;
        continue;
      }
      insert(0, span, checkpointTs);
    }
  }

  @Override
  public long frontier() {
    return minTsHeap.getMinKey();
  }

  @Override
  public void forward(long regionID, Coprocessor.KeyRange span, long ts) {
    SkipListNode node = cachedRegions.get(regionID);
    if (node != null
        && node.getRegion() == regionID
        && Arrays.equals(node.getKey(), span.getStart().toByteArray())
        && Arrays.equals(node.getEnd(), span.getEnd().toByteArray())) {
      // Update the timestamp for the region
      minTsHeap.updateKey(node.getValue(), ts);
    }
    insert(regionID, span, ts);
  }

  private void insert(long regionId, Coprocessor.KeyRange span, long ts) {
    // Insert or update the span in the list
    //    Arrays.fill(seekTempResult, null);
    SkipListNode[] seekRes = spanList.seek(span.getStart().toByteArray());
    SkipListNode next = seekRes[0].nextAtLevel(0);
    if (next != null) {
      if (BytesUtils.compare(next.getKey(), span.getStart().toByteArray()) == 0
          && BytesUtils.compare(next.getKey(), span.getEnd().toByteArray()) == 0) {
        minTsHeap.updateKey(seekRes[0].getValue(), ts);
        cachedRegions.remove(regionId);
        if (regionId != FAKE_REGION_ID) {
          seekRes[0].setRegion(regionId);
          seekRes[0].setEnd(next.getKey());
          cachedRegions.put(regionId, seekRes[0]);
        }
        return;
      }
    }
    SkipListNode node = seekRes[0];
    cachedRegions.remove(regionId);
    long lastNodeTs = Long.MAX_VALUE;
    boolean shouldInsertStartNode = true;
    if (node.getValue() != null) {
      lastNodeTs = node.getValue().getKey();
    }

    for (; node != null; node = node.nextAtLevel(0)) {
      cachedRegions.remove(regionId);
      int cmpStart = BytesUtils.compare(node.getKey(), span.getStart().toByteArray());
      if (cmpStart < 0) {
        continue;
      }
      if (BytesUtils.compare(node.getKey(), span.getEnd().toByteArray()) > 0) {
        continue;
      }
      lastNodeTs = node.getValue().getKey();
      if (cmpStart == 0) {
        minTsHeap.updateKey(node.getValue(), ts);
        shouldInsertStartNode = false;
      } else {
        SkipListNode[] seekNodes = spanList.seek(node.getKey());
        spanList.remove(seekNodes, node);
        minTsHeap.remove(node.getValue());
      }
      if (shouldInsertStartNode) {
        spanList.insertNextToNode(seekRes, span.getStart().toByteArray(), minTsHeap.insert(ts));
        seekRes = next(seekRes);
      }
      spanList.insertNextToNode(seekRes, span.getEnd().toByteArray(), minTsHeap.insert(lastNodeTs));
    }
  }

  public SkipListNode[] next(SkipListNode[] seekResult) {
    SkipListNode next = seekResult[0].nextAtLevel(0);
    for (int i = 0; i < next.next().length; i++) {
      seekResult[i] = next.next()[i];
    }
    return seekResult;
  }

  @Override
  public void entries(FrontierConsumer fn) {
    spanList.entries(
        (node) -> {
          fn.accept(node.getKey(), node.getValue().getKey());
          return true;
        });
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    this.entries(
        (key, ts) -> {
          if (ts == Long.MAX_VALUE) {
            buf.append(String.format("[%s @ Max] ", new String(key)));
          } else {
            buf.append(String.format("[%s @ %d] ", new String(key), ts));
          }
        });
    return buf.toString();
  }
}
