package org.tikv.cdc.model;

import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class RegionTsManager {
  private final Map<Long, RegionTsInfo> regionTsMap;
  private final PriorityQueue<RegionTsInfo> regionTsHeap;

  public RegionTsManager() {
    this.regionTsMap = new HashMap<>();
    this.regionTsHeap = new PriorityQueue<>(Comparator.comparingLong(r -> r.ts.resolvedTs));
  }

  // Upsert method: insert or update resolvedTs information for the given regionID
  public void upsert(long regionID, long resolvedTs, Instant eventTime) {
    RegionTsInfo old = regionTsMap.get(regionID);
    if (old != null) {
      // Handle fallback resolved event and increase penalty
      if (resolvedTs <= old.ts.resolvedTs && eventTime.isAfter(old.ts.eventTime)) {
        old.ts.penalty++;
        old.ts.eventTime = eventTime;
      } else if (resolvedTs > old.ts.resolvedTs) {
        old.ts.resolvedTs = resolvedTs;
        old.ts.eventTime = eventTime;
        old.ts.penalty = 0;
        // Reorder the heap
        regionTsHeap.remove(old);
        regionTsHeap.offer(old);
      }
    } else {
      RegionTsInfo item = new RegionTsInfo(regionID, new TsItem(resolvedTs, eventTime, 0));
      insert(item);
    }
  }

  // Insert a new RegionTsInfo into the priority queue and map
  public void insert(RegionTsInfo item) {
    regionTsHeap.offer(item);
    regionTsMap.put(item.regionID, item);
  }

  // Pop the region with the lowest resolvedTs and remove it from the map
  public RegionTsInfo pop() {
    if (regionTsHeap.isEmpty()) {
      return null;
    }
    RegionTsInfo item = regionTsHeap.poll();
    regionTsMap.remove(item.regionID);
    return item;
  }

  // Remove the regionTsInfo for the specified regionID
  public RegionTsInfo remove(long regionID) {
    RegionTsInfo item = regionTsMap.get(regionID);
    if (item != null) {
      regionTsMap.remove(regionID);
      regionTsHeap.remove(item);
      return item;
    }
    return null;
  }

  // Get the number of regions in the manager
  public int size() {
    return regionTsMap.size();
  }

  // RegionTsInfo stores region-specific resolvedTs information
  public static class RegionTsInfo {
    private final long regionID;
    private TsItem ts;

    public RegionTsInfo(long regionID, TsItem ts) {
      this.regionID = regionID;
      this.ts = ts;
    }
  }

  // TsItem stores the resolvedTs, eventTime, and penalty for a region
  public static class TsItem {
    private long resolvedTs;
    private Instant eventTime;
    private int penalty;

    public TsItem(long resolvedTs, Instant eventTime, int penalty) {
      this.resolvedTs = resolvedTs;
      this.eventTime = eventTime;
      this.penalty = penalty;
    }
  }
}
