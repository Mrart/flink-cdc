/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tikv.common.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.HostMapping;
import org.tikv.common.pd.PDUtils;
import org.tikv.shade.io.grpc.ManagedChannel;
import org.tikv.shade.io.grpc.netty.GrpcSslContexts;
import org.tikv.shade.io.grpc.netty.NettyChannelBuilder;
import org.tikv.shade.io.netty.channel.Channel;
import org.tikv.shade.io.netty.channel.MultithreadEventLoopGroup;
import org.tikv.shade.io.netty.channel.epoll.Epoll;
import org.tikv.shade.io.netty.channel.epoll.EpollEventLoopGroup;
import org.tikv.shade.io.netty.channel.epoll.EpollSocketChannel;
import org.tikv.shade.io.netty.channel.nio.NioEventLoopGroup;
import org.tikv.shade.io.netty.channel.socket.nio.NioSocketChannel;
import org.tikv.shade.io.netty.handler.ssl.SslContext;
import org.tikv.shade.io.netty.handler.ssl.SslContextBuilder;
import org.tikv.shade.io.netty.util.concurrent.FastThreadLocalThread;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Copied from https://github.com/tikv/client-java project to fix
 * https://github.com/tikv/client-java/issues/600 for 3.2.0 version.
 */
public class ChannelFactory implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ChannelFactory.class);

    private final int maxFrameSize;
    private final int keepaliveTime;
    private final int keepaliveTimeout;
    private final int idleTimeout;
    private final ConcurrentHashMap<String, ManagedChannel> connPool = new ConcurrentHashMap<>();
    private final SslContextBuilder sslContextBuilder;
    private static final String PUB_KEY_INFRA = "PKIX";

    public ChannelFactory(
            int maxFrameSize, int keepaliveTime, int keepaliveTimeout, int idleTimeout) {
        this.maxFrameSize = maxFrameSize;
        this.keepaliveTime = keepaliveTime;
        this.keepaliveTimeout = keepaliveTimeout;
        this.idleTimeout = idleTimeout;
        this.sslContextBuilder = null;
    }

    public ChannelFactory(
            int maxFrameSize,
            int keepaliveTime,
            int keepaliveTimeout,
            int idleTimeout,
            String trustCertCollectionFilePath,
            String keyCertChainFilePath,
            String keyFilePath) {
        this.maxFrameSize = maxFrameSize;
        this.keepaliveTime = keepaliveTime;
        this.keepaliveTimeout = keepaliveTimeout;
        this.idleTimeout = idleTimeout;
        this.sslContextBuilder =
                getSslContextBuilder(
                        trustCertCollectionFilePath, keyCertChainFilePath, keyFilePath);
    }

    public ChannelFactory(
            int maxFrameSize,
            int keepaliveTime,
            int keepaliveTimeout,
            int idleTimeout,
            String jksKeyPath,
            String jksKeyPassword,
            String jkstrustPath,
            String jksTrustPassword) {
        this.maxFrameSize = maxFrameSize;
        this.keepaliveTime = keepaliveTime;
        this.keepaliveTimeout = keepaliveTimeout;
        this.idleTimeout = idleTimeout;
        this.sslContextBuilder =
                getSslContextBuilder(jksKeyPath, jksKeyPassword, jkstrustPath, jksTrustPassword);
    }

    private SslContextBuilder getSslContextBuilder(
            String jksKeyPath,
            String jksKeyPassword,
            String jksTrustPath,
            String jksTrustPassword) {
        SslContextBuilder builder = GrpcSslContexts.forClient();
        try {
            if (jksKeyPath != null && jksKeyPassword != null) {
                KeyStore keyStore = KeyStore.getInstance("JKS");
                keyStore.load(new FileInputStream(jksKeyPath), jksKeyPassword.toCharArray());
                KeyManagerFactory keyManagerFactory =
                        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, jksKeyPassword.toCharArray());
                builder.keyManager(keyManagerFactory);
            }
            if (jksTrustPath != null && jksTrustPassword != null) {
                KeyStore trustStore = KeyStore.getInstance("JKS");
                trustStore.load(new FileInputStream(jksTrustPath), jksTrustPassword.toCharArray());
                TrustManagerFactory trustManagerFactory =
                        TrustManagerFactory.getInstance(PUB_KEY_INFRA);
                trustManagerFactory.init(trustStore);
                builder.trustManager(trustManagerFactory);
            }
        } catch (Exception e) {
            logger.error("JKS SSL context builder failed!", e);
        }
        return builder;
    }

    private SslContextBuilder getSslContextBuilder(
            String trustCertCollectionFilePath, String keyCertChainFilePath, String keyFilePath) {
        SslContextBuilder builder = GrpcSslContexts.forClient();
        if (trustCertCollectionFilePath != null) {
            builder.trustManager(new File(trustCertCollectionFilePath));
        }
        if (keyCertChainFilePath != null && keyFilePath != null) {
            builder.keyManager(new File(keyCertChainFilePath), new File(keyFilePath));
        }
        return builder;
    }

    protected static final class EventThread extends FastThreadLocalThread {
        public EventThread(Runnable r) {
            super(r);
        }
    }

    public ManagedChannel getChannel(String addressStr, HostMapping hostMapping) {
        // todo check channel state
        return connPool.computeIfAbsent(
                addressStr,
                key -> {
                    URI address;
                    URI mappedAddr;
                    try {
                        address = PDUtils.addrToUri(key);
                    } catch (Exception e) {
                        throw new IllegalArgumentException("failed to form address " + key, e);
                    }
                    try {
                        mappedAddr = hostMapping.getMappedURI(address);
                    } catch (Exception e) {
                        throw new IllegalArgumentException(
                                "failed to get mapped address " + address, e);
                    }

                    Class<? extends Channel> channelType;
                    ThreadFactory tfac =
                            new ThreadFactoryBuilder()
                                    .setDaemon(true)
                                    .setThreadFactory(EventThread::new)
                                    .setNameFormat("event-channel-pool-%d")
                                    .build();
                    MultithreadEventLoopGroup internalExecutor;
                    if (Epoll.isAvailable()) {
                        internalExecutor = new EpollEventLoopGroup(4, tfac);
                        channelType = EpollSocketChannel.class;
                    } else {
                        internalExecutor = new NioEventLoopGroup(4, tfac);
                        channelType = NioSocketChannel.class;
                    }

                    // Channel should be lazy without actual connection until first call
                    // So a coarse grain lock is ok here
                    NettyChannelBuilder builder =
                            NettyChannelBuilder.forAddress(
                                    mappedAddr.getHost(), mappedAddr.getPort());
                    Map<String, ?> serviceConfig = getServiceConfig();

                    builder.maxInboundMessageSize(maxFrameSize)
                            .keepAliveTime(keepaliveTime, TimeUnit.SECONDS)
                            .keepAliveTimeout(keepaliveTimeout, TimeUnit.SECONDS)
                            .keepAliveWithoutCalls(true)
                            .idleTimeout(idleTimeout, TimeUnit.SECONDS)
                            .defaultServiceConfig(serviceConfig)
                            .eventLoopGroup(internalExecutor)
                            .channelType(channelType)
                            .enableRetry();

                    try {
                        if (sslContextBuilder == null) {
                            return builder.usePlaintext().build();
                        } else {
                            SslContext sslContext = sslContextBuilder.build();
                            return builder.sslContext(sslContext).build();
                        }
                    } catch (SSLException e) {
                        logger.error("create ssl context failed!", e);
                        return null;
                    }
                });
    }

    public static Map<String, List<Map<String, Object>>> getServiceConfig() {
        // Retry policy
        Map<String, Object> retryPolicy = new HashMap<>();
        retryPolicy.put("maxAttempts", "50");
        retryPolicy.put("initialBackoff", "0.5s");
        retryPolicy.put("maxBackoff", "30s");
        retryPolicy.put("backoffMultiplier", "2");
        retryPolicy.put("retryableStatusCodes", Arrays.asList("UNAVAILABLE", "DEADLINE_EXCEEDED"));

        // Name configuration
        Map<String, Object> nameConfig = new HashMap<>();
        nameConfig.put("service", "cdcpb.ChangeData");
        nameConfig.put("method", "EventFeed");

        // Method configuration
        Map<String, Object> methodConfig = new HashMap<>();
        methodConfig.put("name", Arrays.asList(nameConfig));
        methodConfig.put("retryPolicy", retryPolicy);

        // Service configuration
        Map<String, List<Map<String, Object>>> serviceConfig = new HashMap<>();
        serviceConfig.put("methodConfig", Arrays.asList(methodConfig));

        return serviceConfig;
    }

    @Override
    public void close() {
        for (ManagedChannel ch : connPool.values()) {
            ch.shutdown();
        }
        connPool.clear();
    }
}
