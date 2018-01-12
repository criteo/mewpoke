package com.criteo.nosql.mewpoke.couchbase;

import com.criteo.nosql.mewpoke.discovery.Service;
import com.criteo.nosql.mewpoke.prometheus.MetaCollectorRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;

import java.net.InetSocketAddress;
import java.util.Map;

public class CouchbaseMetrics implements AutoCloseable {

    private final CollectorRegistry registry = new CollectorRegistry(true);
    private final Gauge UP = Gauge.build()
            .name("couchbase_up")
            .help("Are the servers up?")
            .labelNames("cluster", "bucket", "instance")
            .register(registry);

    private final Summary LATENCY = Summary.build()
            .name("couchbase_latency")
            .help("latencies observed by instance and command")
            .labelNames("cluster", "bucket", "instance", "command")
            .maxAgeSeconds(5 * 60)
            .ageBuckets(5)
            .quantile(0.5, 0.05)
            .quantile(0.9, 0.01)
            .quantile(0.99, 0.001)
            .register(registry);

    private final Gauge OPERATIONS = Gauge.build()
            .name("couchbase_operations")
            .help("Cluster ongoing operations")
            .labelNames("cluster", "operation")
            .register(registry);

    private final Gauge MEMBERSHIP = Gauge.build()
            .name("couchbase_membership")
            .help("Cluster membership status")
            .labelNames("cluster", "membership")
            .register(registry);

    private final Gauge STATS = Gauge.build()
            .name("couchbase_stats")
            .help("Cluster API Stats")
            .labelNames("cluster", "bucket", "instance", "name")
            .register(registry);

    private final Gauge XDCR = Gauge.build()
            .name("couchbase_xdcr")
            .help("Cluster API Stats XDCR")
            .labelNames("cluster", "bucket", "remotecluster", "name")
            .register(registry);

    private final String clusterName;
    private final String bucketName;


    public CouchbaseMetrics(final Service service) {
        this.clusterName = service.getClusterName();
        this.bucketName = service.getBucketName().replace('.', '_');

        MetaCollectorRegistry.metaRegistry.register(this.registry);
    }

    public void updateRebalanceOps(final boolean rebalanceOngoing) {
        OPERATIONS.labels(clusterName, "rebalance")
                .set(rebalanceOngoing ? 1 : 0);
    }

    public void updateAvailability(final Map<InetSocketAddress, Boolean> availabilities) {
        availabilities.forEach((addr, availability) -> {
            UP.labels(clusterName, bucketName, addr.getHostName())
                    .set(availability ? 1 : 0);
        });
    }

    public void updateMembership(final Map<InetSocketAddress, String> memberships) {
        MEMBERSHIP.labels(clusterName, "active")
                .set(memberships.values().stream().filter(v -> v.startsWith("active")).count());
        MEMBERSHIP.labels(clusterName, "inactive")
                .set(memberships.values().stream().filter(v -> !v.startsWith("active")).count());
    }

    public void updateDiskLatency(final Map<InetSocketAddress, Long> latencies) {
        latencies.forEach((statname, latency) -> {
            LATENCY.labels(clusterName, bucketName, statname.getHostName(), "persistToDisk")
                    .observe(latency);
        });
    }

    public void updatecollectApiStatsBucket(final Map<InetSocketAddress, Map<String, Double>> nodesStats) {
        nodesStats.forEach((addr, stats) -> {
            stats.forEach((statname, statvalue) -> {
                STATS.labels(clusterName, bucketName, addr.getHostName(), statname)
                        .set(statvalue);
            });
        });
    }

    public void updatecollectApiStatsBucketXdcr(final Map<String, Map<String, Double>> nodesXdcrStats) {
        nodesXdcrStats.forEach((remotecluster, stats) -> {
            stats.forEach((statname, statvalue) -> {
                XDCR.labels(clusterName, bucketName, remotecluster, statname)
                        .set(statvalue);
            });
        });
    }

    @Override
    public void close() {
        // TODO: Not sure if all those clear on collector are necessary
        // but as I found no information in prometheus regarding if it keeps some references
        // So let be safe, and clean everything
        UP.clear();
        LATENCY.clear();
        OPERATIONS.clear();
        MEMBERSHIP.clear();
        STATS.clear();
        XDCR.clear();
        registry.clear();

        MetaCollectorRegistry.metaRegistry.unregister(registry);
    }
}
