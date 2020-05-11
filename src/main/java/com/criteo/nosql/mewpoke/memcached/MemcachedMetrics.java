package com.criteo.nosql.mewpoke.memcached;

import com.criteo.nosql.mewpoke.discovery.Service;
import com.google.common.base.Splitter;
import com.google.common.primitives.Doubles;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public class MemcachedMetrics implements AutoCloseable {

    private static final Gauge UP = Gauge.build()
            .name("memcached_up")
            .help("Availability of a given bucket on a given Memcached node")
            .labelNames("cluster", "bucket", "instance")
            .register();

    private static final Summary LATENCY = Summary.build()
            .name("memcached_latency")
            .help("Get and Set latencies")
            .labelNames("cluster", "bucket", "instance", "command")
            .maxAgeSeconds(5 * 60)
            .ageBuckets(5)
            .quantile(0.5, 0.05)
            .quantile(0.9, 0.01)
            .quantile(0.99, 0.001)
            .register();

    private static final Gauge STATS = Gauge.build()
            .name("memcached_stats")
            .help("Memcached global stats (ex: total_items, get_misses,..etc.)")
            .labelNames("cluster", "bucket", "instance", "name")
            .register();

    private static final Gauge STATSITEMS = Gauge.build()
            .name("memcached_items")
            .help("Memcached slab specific metrics (ex: moves_to_warm, evicted,..etc.)")
            .labelNames("cluster", "bucket", "instance", "slabsizerange", "name")
            .register();

    private final String clusterName;
    private final String bucketName;


    public MemcachedMetrics(final Service service) {
        this.clusterName = service.getClusterName();
        this.bucketName = service.getBucketName().replace('.', '_');
    }


    public void updateStats(final Map<InetSocketAddress, Map<String, String>> nodesStats) {
        nodesStats.forEach((addr, stats) -> {
            stats.forEach((statname, statvalue) -> {
                final Double val = Doubles.tryParse(statvalue);
                STATS.labels(clusterName, bucketName, addr.getHostName(), statname)
                        .set(val == null ? Double.NaN : val);
            });
        });
    }

    public void updateStatsItems(final Map<InetSocketAddress, Map<String, String>> nodesStats) {
        nodesStats.forEach((addr, stats) -> {
            stats.forEach((statname, statvalue) -> {
                List<String> listStatname = Splitter.on(':').splitToList(statname);
                final String slabSizeRange = listStatname.get(1);
                final String name = listStatname.get(2);
                final Double val = Doubles.tryParse(statvalue);
                STATSITEMS.labels(clusterName, bucketName, addr.getHostName(), slabSizeRange, name)
                        .set(val == null ? Double.NaN : val);
            });
        });
    }

    public void updateAvailability(final Map<InetSocketAddress, Map<String, String>> nodesStats) {
        nodesStats.forEach((addr, stats) -> {
            UP.labels(clusterName, bucketName, addr.getHostName())
                    .set(Math.min(stats.size(), 1));
        });
    }

    public void updateGetLatency(final Map<InetSocketAddress, Long> latencies) {
        latencies.forEach((addr, latency) -> {
            LATENCY.labels(clusterName, bucketName, addr.getHostName(), "get")
                    .observe(latency);
        });
    }

    public void updateSetLatency(final Map<InetSocketAddress, Long> latencies) {
        latencies.forEach((addr, latency) -> {
            LATENCY.labels(clusterName, bucketName, addr.getHostName(), "set")
                    .observe(latency);
        });
    }

    @Override
    public void close() {
        // FIXME we should remove only metrics with the labels cluster=clustername, bucket=bucketName
        UP.clear();
        LATENCY.clear();
        STATS.clear();
        STATSITEMS.clear();
    }
}
