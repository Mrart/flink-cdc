package org.tikv.cdc;

import org.tikv.kvproto.ChangeDataGrpc;
import org.tikv.shade.io.grpc.ManagedChannel;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

public class EventFeedStream implements AutoCloseable {
    private final String addr;
    private final long storeId;
    private final ManagedChannel channel;
    private final ChangeDataGrpc.ChangeDataStub asyncStub;
    private final long streamId;
    private final RegionStateManager.SyncRegionFeedStateMap regions;
    private final Instant createTime;
    private AtomicBoolean isCanceled = new AtomicBoolean(false);

    public EventFeedStream(String addr, long storeId, ManagedChannel channel) {
        this.addr = addr;
        this.storeId = storeId;
        this.channel = channel;
        this.asyncStub = ChangeDataGrpc.newStub(channel);
        this.streamId = IDAllocator.allocateStreamClientID();
        this.regions = new RegionStateManager.SyncRegionFeedStateMap();
        this.createTime = Instant.now();
    }

    public String getAddr() {
        return addr;
    }

    public long getStoreId() {
        return storeId;
    }

    public ManagedChannel getChannel() {
        return channel;
    }

    public ChangeDataGrpc.ChangeDataStub getAsyncStub() {
        return asyncStub;
    }

    public long getStreamId() {
        return streamId;
    }

    public boolean getIsCanceled() {
        return isCanceled.get();
    }

    public void setIsCanceled(boolean isCanceled) {
        this.isCanceled.set(isCanceled);
    }

    public RegionStateManager.SyncRegionFeedStateMap getRegions() {
        return regions;
    }

    public Instant getCreateTime() {
        return createTime;
    }

    @Override
    public void close() throws Exception {
        this.setIsCanceled(true);
    }
}
