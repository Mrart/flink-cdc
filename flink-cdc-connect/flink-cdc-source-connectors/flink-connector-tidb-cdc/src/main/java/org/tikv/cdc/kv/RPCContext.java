package org.tikv.cdc.kv;

import org.tikv.common.HostMapping;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.ChannelFactory;
import org.tikv.kvproto.Metapb;
import org.tikv.shade.io.grpc.ManagedChannel;

public class RPCContext {
    private final TiRegion.RegionVerID region;
    private final Metapb.Region meta;
    private final Metapb.Peer peer;
    private final TiStore tiStore;
    private final String address;
    private final HostMapping hostMapping;
    private final ChannelFactory channelFactory;

    // Private constructor to enforce the use of the Builder
    private RPCContext(Builder builder) {
        this.region = builder.region;
        this.meta = builder.meta;
        this.peer = builder.peer;
        this.tiStore = builder.tiStore;
        this.address = builder.address;
        this.hostMapping = builder.hostMapping;
        this.channelFactory = builder.channelFactory;
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

    public HostMapping getHostMapping() {
        return hostMapping;
    }

    public TiStore getTiStore() {
        return tiStore;
    }

    public String getAddress() {
        return address;
    }

    public ChannelFactory getChannelFactory() {
        return channelFactory;
    }

    public ManagedChannel getChannel() {
        return this.channelFactory.getChannel(this.address, this.hostMapping);
    }

    public RPCContext withNewAddress(String newAddress) {
        return new Builder()
                .setRegion(this.region)
                .setMeta(this.meta)
                .setPeer(this.peer)
                .setTiStore(this.tiStore)
                .setAddress(newAddress)
                .setChannel(this.channelFactory)
                .setHostMapping(this.hostMapping)
                .build();
    }

    public RPCContext withNewStore(TiStore store) {
        return new Builder()
                .setRegion(this.region)
                .setMeta(this.meta)
                .setPeer(this.peer)
                .setTiStore(store)
                .setAddress(this.address)
                .setChannel(this.channelFactory)
                .setHostMapping(this.hostMapping)
                .build();
    }
    // Builder class
    public static class Builder {
        private TiRegion.RegionVerID region;
        private Metapb.Region meta;
        private Metapb.Peer peer;
        private TiStore tiStore;
        private String address;
        private HostMapping hostMapping;
        private ChannelFactory channelFactory;

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

        public Builder setChannel(ChannelFactory channel) {
            this.channelFactory = channel;
            return this;
        }

        public Builder setHostMapping(HostMapping hostMapping) {
            if (hostMapping != null) {
                this.hostMapping = hostMapping;
            }
            return this;
        }

        public RPCContext build() {
            return new RPCContext(this);
        }
    }
}
