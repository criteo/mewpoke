package com.criteo.nosql.mewpoke.memcached;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.criteo.nosql.mewpoke.config.Config;
import com.ecwid.consul.v1.health.model.HealthService;

public class MemcachedRunnerLatency extends MemcachedRunnerAbstract {
    public MemcachedRunnerLatency(final Config cfg) {
        super(cfg);
    }

    public void poke() {
        this.monitors.forEach((service, monitor) -> {
            final MemcachedMetrics metric = metrics.get(service);
            metric.updateSetLatency(monitor.map(MemcachedMonitor::collectSetLatencies).orElse(Collections.emptyMap()));
            metric.updateGetLatency(monitor.map(MemcachedMonitor::collectGetLatencies).orElse(Collections.emptyMap()));
            metric.updateAvailability(monitor.map(MemcachedMonitor::collectStats).orElse(Collections.emptyMap()));
        });
    }
}
