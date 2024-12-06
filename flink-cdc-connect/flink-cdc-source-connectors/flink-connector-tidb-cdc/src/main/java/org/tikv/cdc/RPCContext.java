package org.tikv.cdc;

import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.kvproto.Metapb;
import org.tikv.shade.io.grpc.ManagedChannel;

public class RPCContext {
    private final TiRegion.RegionVerID region;
    private final Metapb.Region meta;
    private final Metapb.Peer peer;
    private final TiStore tiStore;
    private final String address;
    private final ManagedChannel channel;

    // Private constructor to enforce the use of the Builder
    private RPCContext(Builder builder) {
        this.region = builder.region;
        this.meta = builder.meta;
        this.peer = builder.peer;
        this.tiStore = builder.tiStore;
        this.address = builder.address;
        this.channel = builder.managedChannel;
    }

    // Getters for the fields (optional)
    public TiRegion.RegionVerID getRegion() {
        return region;
    }

    public Metapb.Region getMeta() {
        return meta;
    }

    public Metapb.Peer getPeer() {
        return peer;
    }

    public TiStore getTiStore() {
        return tiStore;
    }

    public String getAddress() {
        return address;
    }

    public ManagedChannel getChannel() {
        return channel;
    }

    // Builder class
    public static class Builder {
        private TiRegion.RegionVerID region;
        private Metapb.Region meta;
        private Metapb.Peer peer;
        private TiStore tiStore;
        private String address;
        private ManagedChannel managedChannel;

        public Builder setRegion(TiRegion.RegionVerID region) {
            this.region = region;
            return this;
        }

        public Builder setMeta(Metapb.Region meta) {
            this.meta = meta;
            return this;
        }

        public Builder setPeer(Metapb.Peer peer) {
            this.peer = peer;
            return this;
        }

        public Builder setTiStore(TiStore tiStore) {
            this.tiStore = tiStore;
            return this;
        }

        public Builder setAddress(String address) {
            this.address = address;
            return this;
        }

        public Builder setChannel(ManagedChannel channel) {
            this.managedChannel = channel;
            return this;
        }

        public RPCContext build() {
            return new RPCContext(this);
        }
    }
}
