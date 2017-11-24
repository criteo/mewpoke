package com.criteo.nosql.mewpoke.memcached;

import com.criteo.nosql.mewpoke.config.Config;

import java.util.Collections;

public class MemcachedRunnerStats extends MemcachedRunnerAbstract
{
    public MemcachedRunnerStats(final Config cfg)
    {
        super(cfg);
    }

    public void poke()
    {
        this.monitors.forEach((service, monitor) -> {
            final MemcachedMetrics metric = metrics.get(service);
            metric.updateStats(monitor.map(MemcachedMonitor::collectStats).orElse(Collections.emptyMap()));
        });
    }
}
