package com.criteo.nosql.mewpoke.memcached;

import com.criteo.nosql.mewpoke.discovery.Service;
import com.google.common.primitives.Doubles;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;

import java.net.InetSocketAddress;
import java.util.Map;

public class MemcachedMetrics implements AutoCloseable {

    static final Gauge UP = Gauge.build()
            .name("memcached_up")
            .help("Are the servers up?")
            .labelNames("cluster", "bucket", "instance")
            .register();

    static final Summary LATENCY = Summary.build()
            .name("memcached_latency")
            .help("latencies observed by instance and command")
            .labelNames("cluster", "bucket", "instance", "command")
            .maxAgeSeconds(60)
            .ageBuckets(1)
            .quantile(0.5, 0.05)
            .quantile(0.9, 0.01)
            .quantile(0.99, 0.001)
            .register();

    static final Gauge STATS = Gauge.build()
            .name("memcached_stats")
            .help("get global stats")
            .labelNames("cluster", "bucket", "instance", "name")
            .create().register();

    static final Gauge STATSITEMS = Gauge.build()
            .name("memcached_items")
            .help("Get items statistics by slab ID")
            .labelNames("cluster", "bucket", "instance", "slabid", "name")
            .create().register();

    private final String clusterName;
    private final String bucketName;


    public MemcachedMetrics(final Service service) {
        this.clusterName = service.getClusterName();
        this.bucketName = service.getBucketName().replace('.', '_');
    }


    public void updateStats(final Map<InetSocketAddress, Map<String, String>> nodesStats) {
        nodesStats.forEach((addr, stats) -> {
            stats.forEach((statname, statvalue) -> {
                Double val = Doubles.tryParse(statvalue);
                STATS.labels(clusterName, bucketName, addr.getHostName(), statname).set(val == null ? Double.NaN : val);
            });
        });
    }

    public void updateStatsItems(final Map<InetSocketAddress, Map<String, String>> nodesStats) {
        nodesStats.forEach((addr, stats) -> {
            stats.forEach((statname, statvalue) -> {
                String slabId = statname.split(":")[1];
                String name = statname.split(":")[2];
                Double val = Doubles.tryParse(statvalue);
                STATSITEMS.labels(clusterName, bucketName, addr.getHostName(), slabId, name).set(val == null ? Double.NaN : val);
            });
        });
    }

    public void updateAvailability(final Map<InetSocketAddress, Map<String, String>> nodesStats) {
        nodesStats.forEach((addr, stats) -> {
            UP.labels(clusterName, bucketName, addr.getHostName()).set(Math.min(stats.size(), 1));
        });
    }

    public void updateGetLatency(final Map<InetSocketAddress, Long> latencies) {
        latencies.forEach((addr, latency) -> {
            LATENCY.labels(clusterName, bucketName, addr.getHostName(), "get").observe(latency);
        });
    }

    public void updateSetLatency(final Map<InetSocketAddress, Long> latencies) {
        latencies.forEach((addr, latency) -> {
            LATENCY.labels(clusterName, bucketName, addr.getHostName(), "set").observe(latency);
        });
    }

    @Override
    public void close() {
        UP.clear();
        LATENCY.clear();
        STATS.clear();
        STATSITEMS.clear();
    }
}
