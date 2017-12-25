package com.criteo.nosql.mewpoke.memcached;

import com.criteo.nosql.mewpoke.config.Config;
import com.criteo.nosql.mewpoke.discovery.IDiscovery;

import java.util.Collections;

public class MemcachedRunnerStats extends MemcachedRunnerAbstract {
    public MemcachedRunnerStats(final Config cfg, final IDiscovery discovery) {
        super(cfg, discovery);
    }

    public void poke() {
        this.monitors.forEach((service, monitor) -> {
            final MemcachedMetrics metric = metrics.get(service);
            metric.updateStats(monitor.map(MemcachedMonitor::collectStats).orElse(Collections.emptyMap()));
        });
    }
}
