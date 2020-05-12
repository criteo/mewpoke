package com.criteo.nosql.mewpoke.couchbase;

import com.criteo.nosql.mewpoke.discovery.Service;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;

import java.net.InetSocketAddress;
import java.util.Map;

public class CouchbaseMetrics implements AutoCloseable {

    private static final Gauge UP = Gauge.build()
            .name("couchbase_up")
            .help("Availability of a given bucket on a given Couchbase node")
            .labelNames("cluster", "bucket", "instance")
            .register();

    private static final Gauge OPERATIONS = Gauge.build()
            .name("couchbase_operations")
            .help("Ongoing rebalance operatio on the cluster")
            .labelNames("cluster", "operation")
            .register();

    private static final Gauge MEMBERSHIP = Gauge.build()
            .name("couchbase_membership")
            .help("Cluster membership status")
            .labelNames("cluster", "membership")
            .register();

    private static final Gauge STATS = Gauge.build()
            .name("couchbase_stats")
            .help("Couchbase specific stats that are listed in the config file in the couchbaseStats/bucket section")
            .labelNames("cluster", "bucket", "instance", "name")
            .register();

    private static final Gauge XDCR = Gauge.build()
            .name("couchbase_xdcr")
            .help("Couchbase replication stats that are listed in the config file in the couchbaseStats/xdcr section")
            .labelNames("cluster", "bucket", "remotecluster", "name")
            .register();

    private final String clusterName;
    private final String bucketName;


    public CouchbaseMetrics(final Service service) {
        this.clusterName = service.getClusterName();
        this.bucketName = service.getBucketName().replace('.', '_');
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
        // FIXME we should remove only metrics with the labels cluster=clustername, bucket=bucketName
        UP.clear();
        OPERATIONS.clear();
        MEMBERSHIP.clear();
        STATS.clear();
        XDCR.clear();
    }
}
