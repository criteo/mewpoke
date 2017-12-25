package com.criteo.nosql.mewpoke.couchbase;

import java.util.Collections;

import com.criteo.nosql.mewpoke.config.Config;
import com.criteo.nosql.mewpoke.discovery.IDiscovery;

public class CouchbaseRunnerStats extends CouchbaseRunnerAbstract {

    public CouchbaseRunnerStats(final Config cfg, final IDiscovery discovery) {
        super(cfg, discovery);
    }

    @Override
    public void poke() {
        this.monitors.forEach((service, monitor) -> {
            final CouchbaseMetrics metric = this.metrics.get(service);
            metric.updateRebalanceOps(monitor.map(CouchbaseMonitor::collectRebalanceOps).orElse(Boolean.FALSE));
            metric.updateMembership(monitor.map(CouchbaseMonitor::collectMembership).orElse(Collections.emptyMap()));
            metric.updatecollectApiStatsBucket(monitor.map(CouchbaseMonitor::collectApiStatsBucket).orElse(Collections.emptyMap()));
            metric.updatecollectApiStatsBucketXdcr(monitor.map(CouchbaseMonitor::collectApiStatsBucketXdcr).orElse(Collections.emptyMap()));
        });
    }
}
