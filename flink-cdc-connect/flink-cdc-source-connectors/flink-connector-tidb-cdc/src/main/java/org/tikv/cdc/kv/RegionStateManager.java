package org.tikv.cdc.kv;

import org.tikv.common.region.TiRegion;
import org.tikv.kvproto.Coprocessor.KeyRange;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RegionStateManager {

    private static final int MIN_REGION_STATE_BUCKET = 4;
    private static final int MAX_REGION_STATE_BUCKET = 16;

    public static final int STATE_NORMAL = 0;
    public static final int STATE_STOPPED = 1;
    public static final int STATE_REMOVED = 2;

    public static class SingleRegionInfo {
        private final TiRegion.RegionVerID verID;
        private final KeyRange span;
        private final RPCContext rpcCtx;
        private final LockedRange lockedRange;

        public SingleRegionInfo(TiRegion.RegionVerID verID, KeyRange span, RPCContext rpcCtx) {
            this.verID = verID;
            this.span = span;
            this.rpcCtx = rpcCtx;
            this.lockedRange = new LockedRange();
        }

        public long getResolvedTs() {
            return lockedRange.getCheckpointTs();
        }

        public RPCContext getRpcCtx() {
            return this.rpcCtx;
        }

        public KeyRange getSpan() {
            return span;
        }

        public TiRegion.RegionVerID getVerID() {
            return verID;
        }
    }

    public static class RegionFeedState {
        private final SingleRegionInfo sri;
        private final long requestID;
        private Matcher matcher;
        private final State state = new State();

        public RegionFeedState(SingleRegionInfo sri, long requestID) {
            this.sri = sri;
            this.requestID = requestID;
        }

        public void start() {
            this.matcher = new Matcher();
        }

        public void markStopped() {
            state.setStateStopped();
        }

        public Matcher getMatcher() {
            return this.matcher;
        }

        public KeyRange getKeyRange() {
            return this.sri.span;
        }

        public long getRegionId() {
            return this.sri.verID.getId();
        }

        public boolean isStale() {
            return state.isStopped() || state.isRemoved();
        }

        public boolean isInitialized() {
            return sri.lockedRange.isInitialized();
        }

        public void setInitialized() {
            sri.lockedRange.setInitialized(true);
        }

        public TiRegion.RegionVerID getRegionID() {
            return sri.verID;
        }

        public long getLastResolvedTs() {
            return sri.lockedRange.getCheckpointTs();
        }

        public SingleRegionInfo getSri() {
            return sri;
        }

        public void updateResolvedTs(long resolvedTs) {
            sri.lockedRange.updateCheckpointTs(resolvedTs);
        }

        public long getRequestID() {
            return requestID;
        }
    }

    static class State {
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private volatile int v = STATE_NORMAL;

        public void setStateStopped() {
            lock.writeLock().lock();
            try {
                if (v == STATE_NORMAL) {
                    v = STATE_STOPPED;
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        public boolean isStopped() {
            lock.readLock().lock();
            try {
                return v == STATE_STOPPED;
            } finally {
                lock.readLock().unlock();
            }
        }

        public boolean isRemoved() {
            lock.readLock().lock();
            try {
                return v == STATE_REMOVED;
            } finally {
                lock.readLock().unlock();
            }
        }
    }

    public static class SyncRegionFeedStateMap {
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private final ConcurrentHashMap<Long, RegionFeedState> statesInternal =
                new ConcurrentHashMap<>();

        public void setByRequestID(long requestID, RegionFeedState state) {
            lock.writeLock().lock();
            try {
                statesInternal.put(requestID, state);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public RegionFeedState takeByRequestID(long requestID) {
            lock.writeLock().lock();

            try {
                RegionFeedState state = statesInternal.get(requestID);
                if (state != null) {
                    statesInternal.remove(requestID);
                }
                return state;
            } finally {
                lock.writeLock().unlock();
            }
        }

        public void delByRegionID(long regionID) {
            lock.writeLock().lock();
            try {
                statesInternal.remove(regionID);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public int size() {
            lock.readLock().lock();
            try {
                return statesInternal.size();
            } finally {
                lock.readLock().unlock();
            }
        }
    }

    public static class RegionStateManagerImpl {
        private final int bucket;
        private final SyncRegionFeedStateMap[] states;

        public RegionStateManagerImpl(int bucket) {
            this.bucket =
                    Math.min(Math.max(bucket, MIN_REGION_STATE_BUCKET), MAX_REGION_STATE_BUCKET);
            this.states = new SyncRegionFeedStateMap[this.bucket];
            for (int i = 0; i < this.bucket; i++) {
                states[i] = new SyncRegionFeedStateMap();
            }
        }

        private int getBucket(long regionID) {
            return (int) (regionID % bucket);
        }

        public void setState(long regionID, RegionFeedState state) {
            int bucketIndex = getBucket(regionID);
            states[bucketIndex].setByRequestID(regionID, state);
        }

        public RegionFeedState getState(long regionID) {
            int bucketIndex = getBucket(regionID);
            return states[bucketIndex].statesInternal.get(regionID);
        }

        public void delState(long regionID) {
            int bucketIndex = getBucket(regionID);
            states[bucketIndex].delByRegionID(regionID);
        }

        public long regionCount() {
            long count = 0;
            for (SyncRegionFeedStateMap bucket : states) {
                count += bucket.size();
            }
            return count;
        }
    }

    // 简化的辅助类与结构体
    static class LockedRange {
        private final AtomicLong checkpointTs = new AtomicLong(0);
        private final AtomicLong initialized = new AtomicLong(0);

        public long getCheckpointTs() {
            return checkpointTs.get();
        }

        public void updateCheckpointTs(long ts) {
            checkpointTs.updateAndGet(prev -> Math.max(prev, ts));
        }

        public boolean isInitialized() {
            return initialized.get() != 0;
        }

        public void setInitialized(boolean value) {
            initialized.set(value ? 1 : 0);
        }
    }
}
