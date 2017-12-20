package com.criteo.nosql.mewpoke.memcached;

import com.criteo.nosql.mewpoke.config.Config;

import java.util.Collections;

public class MemcachedRunnerStatsItems extends MemcachedRunnerAbstract
{
    public MemcachedRunnerStatsItems(final Config cfg)
    {
        super(cfg);
    }

    public void poke()
    {
        this.monitors.forEach((service, monitor) -> {
            final MemcachedMetrics metric = metrics.get(service);
            metric.updateStatsItems(monitor.map(MemcachedMonitor::collectStatsItems).orElse(Collections.emptyMap()));
        });
    }
}