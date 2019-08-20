package com.criteo.nosql.mewpoke.couchbase;

import java.util.Collections;
import java.util.Optional;

import com.criteo.nosql.mewpoke.config.Config;
import com.criteo.nosql.mewpoke.discovery.IDiscovery;
import com.criteo.nosql.mewpoke.discovery.Service;

public class CouchbaseRunnerLatency extends CouchbaseRunnerAbstract {

    public CouchbaseRunnerLatency(final Config cfg, final IDiscovery discovery) {
        super(cfg, discovery);
    }

    @Override
    public void poke() {
        this.monitors.entrySet().parallelStream().forEach(entry -> {
            final Service service = entry.getKey();
            final Optional<CouchbaseMonitor> monitor = entry.getValue();
            final CouchbaseMetrics metric = this.metrics.get(service);
            metric.updateAvailability(monitor.map(CouchbaseMonitor::collectAvailability).orElse(Collections.emptyMap()));
        });
    }
}
