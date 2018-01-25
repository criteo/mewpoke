package com.criteo.nosql.mewpoke.memcached;

import com.criteo.nosql.mewpoke.discovery.Service;
import net.spy.memcached.*;
import net.spy.memcached.ConnectionFactoryBuilder.Protocol;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StoreOperation;
import net.spy.memcached.ops.StoreType;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MemcachedMonitor implements AutoCloseable {
    private static final String cacheKey = "mempoke_NoSQL";
    private static final int ttlInMs = 60 * 60 * 24 * 15; // should not exceed one month (60*60*24*30)
    private static Logger logger = LoggerFactory.getLogger(MemcachedMonitor.class);
    private final String serviceName;
    private final MemcachedClient client;
    private final BinaryOperationFactory opF;
    private final long timeoutInMs;
    // To avoid too many allocation at each iteration we allocate buffers upfront
    // but as we share results with external world we have to guard against
    // someone mutating our internal buffers, so we freeze the map in order to share them safely
    private final Map<InetSocketAddress, Long> setLatencies;
    private final Map<InetSocketAddress, Long> setLatenciesFrozen;
    private final Map<InetSocketAddress, Long> getLatencies;
    private final Map<InetSocketAddress, Long> getLatenciesFrozen;
    private final CachedData data;

    private MemcachedMonitor(final String serviceName, final MemcachedClient client, final long timeoutInMs) {
        this.serviceName = serviceName;
        this.client = client;
        this.opF = new BinaryOperationFactory();
        this.data = client.getTranscoder().encode("NoSQL for the Win !");
        this.setLatencies = new HashMap<>(client.getConnection().getLocator().getAll().size());
        this.getLatencies = new HashMap<>(client.getConnection().getLocator().getAll().size());
        this.setLatenciesFrozen = Collections.unmodifiableMap(this.setLatencies);
        this.getLatenciesFrozen = Collections.unmodifiableMap(this.getLatencies);
        this.timeoutInMs = timeoutInMs;
    }

    public static Optional<MemcachedMonitor> fromNodes(final Service service, Set<InetSocketAddress> endPoints, long timeoutInMs) {
        if (endPoints.isEmpty()) {
            return Optional.empty();
        }

        try {
            // Create a client based on the binary protocol and drop pending query upon node failure
            final MemcachedClient client = new MemcachedClient(new ConnectionFactoryBuilder(new BinaryConnectionFactory())
                    .setFailureMode(FailureMode.Cancel)
                    .setProtocol(Protocol.BINARY)
                    .setUseNagleAlgorithm(false)
                    .build()
                    , new ArrayList<>(endPoints));
            return Optional.of(new MemcachedMonitor(service.getBucketName(), client, timeoutInMs));
        } catch (IOException e) {
            logger.error("Cannot create memcached client", e);
            return Optional.empty();
        }
    }

    // Memcached driver uses an InetSocketAddress behind the scene, so we cheat on java compiler...
    @SuppressWarnings("unchecked")
    public Map<InetSocketAddress, Map<String, String>> collectStats() {
        // TODO: re-implement get stats in order to avoid creating new map each time
        return (Map) client.getStats();
    }



    public Map<InetSocketAddress, Map<String, String>> collectStatsItems() {
        // TODO: re-implement get stats in order to avoid creating new map each time
        final Map<SocketAddress, Map<String, String>> itemsStats = client.getStats("items");
        final Map<SocketAddress, Map<String, String>> slabsStats = client.getStats("slabs");
        final Map<SocketAddress, Map<String, String>> nodeSettings = client.getStats("settings");

        // We return slab size range instead of slabid
        // Need to re-construct client.getStats
        final Map<SocketAddress, Map<String, String>> itemsStatsByRange = new HashMap<>();

        itemsStats.forEach((socketAddr, itemStats) -> {

            // Construct Map of slabid:slab range size for each node
            final Map<String, Double> minSizeBySlabId = new HashMap<>();
            slabsStats.get(socketAddr).forEach((statName, statValue) -> {
                if(statName.matches("(.*):chunk_size")){
                    String slabId = statName.split(":")[0];
                    Double slabMinSize = Double.valueOf(statValue);
                    minSizeBySlabId.put(slabId, slabMinSize);
                }
            });

            // Replace slabid with slabsize for all metrics
            final Map<String, String> itemStatsByRange = new HashMap<>();
            itemStats.forEach((slabsStatsByIds, statValue) -> {
                final String slabId = slabsStatsByIds.split(":")[1];
                final Double slabMinSize = minSizeBySlabId.get(slabId);
                final Double growthFactor = Double.valueOf(nodeSettings.get(socketAddr).get("growth_factor"));
                final Double slabMaxSize = growthFactor * slabMinSize;

                String slabsStatsByRange = slabsStatsByIds.split(":")[0] + ":"
                        + "[" + slabMinSize.intValue() + "B - "
                        + slabMaxSize.intValue() + "B]:"
                        + slabsStatsByIds.split(":")[2];
                itemStatsByRange.put(slabsStatsByRange, statValue);
            });

            // Return new Map with slabid replaced by slabsize
            itemsStatsByRange.put(socketAddr, itemStatsByRange);
        });
        return (Map) itemsStatsByRange;
    }

    public Map<InetSocketAddress, Long> collectGetLatencies() {
        final CountDownLatch blatch = client.broadcastOp((n, latch) -> {
            final InetSocketAddress sa = (InetSocketAddress) n.getSocketAddress();
            getLatencies.put(sa, timeoutInMs * 1000L);

            final long start = System.nanoTime();
            return opF.get(cacheKey, new GetOperation.Callback() {
                @Override
                public void gotData(String key, int flags, byte[] data) {
                }

                @Override
                public void receivedStatus(OperationStatus status) {
                }

                @Override
                public void complete() {
                    final long stop = System.nanoTime();
                    getLatencies.put(sa, (stop - start) / 1000L);
                    latch.countDown();
                }
            });
        });

        try {
            if (!blatch.await(timeoutInMs, TimeUnit.MILLISECONDS)) {
                logger.error("Collecting get latencies for {} failed to respond in {} ms", serviceName, timeoutInMs);
            }
        } catch (InterruptedException e) {
            logger.error("Collecting get latencies failed for {}, thread Interrupted", serviceName);
        }
        return getLatenciesFrozen;
    }

    public Map<InetSocketAddress, Long> collectSetLatencies() {
        final CountDownLatch blatch = client.broadcastOp((n, latch) -> {
            final InetSocketAddress sa = (InetSocketAddress) n.getSocketAddress();
            setLatencies.put(sa, timeoutInMs * 1000L);

            final long start = System.nanoTime();
            return opF.store(StoreType.set, cacheKey, data.getFlags(), ttlInMs, data.getData(),
                    new StoreOperation.Callback() {
                        @Override
                        public void gotData(String key, long cas) {
                        }

                        @Override
                        public void receivedStatus(OperationStatus status) {
                        }

                        @Override
                        public void complete() {
                            final long stop = System.nanoTime();
                            setLatencies.put(sa, (stop - start) / 1000L);
                            latch.countDown();
                        }
                    });
        });

        try {
            if (!blatch.await(timeoutInMs, TimeUnit.MILLISECONDS)) {
                logger.error("Collecting set latencies for {} failed to respond in {} ms", serviceName, timeoutInMs);
            }
        } catch (InterruptedException e) {
            logger.error("Collecting set latencies failed for {}, thread Interrupted", serviceName);
        }
        return setLatenciesFrozen;
    }

    @Override
    public void close() {
        try {
            client.shutdown();
        } catch (Exception e) {
            logger.error("Cannot shutdown memcached client properly for {}", serviceName, e);
        }
    }
}
