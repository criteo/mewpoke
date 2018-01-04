package com.criteo.nosql.mewpoke.memcached;

import com.criteo.nosql.mewpoke.config.Config;
import com.criteo.nosql.mewpoke.discovery.IDiscovery;
import com.criteo.nosql.mewpoke.discovery.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;

public abstract class MemcachedRunnerAbstract implements AutoCloseable, Runnable {

    private final Logger logger = LoggerFactory.getLogger(MemcachedRunnerAbstract.class);

    private final Config cfg;
    private final IDiscovery discovery;
    private final long measurementPeriodInMs;
    private final long refreshDiscoveryPeriodInMs;

    protected Map<Service, Set<InetSocketAddress>> services;
    protected Map<Service, Optional<MemcachedMonitor>> monitors;
    protected Map<Service, MemcachedMetrics> metrics;

    public MemcachedRunnerAbstract(Config cfg, IDiscovery discovery) {
        this.cfg = cfg;
        this.discovery = discovery;
        this.measurementPeriodInMs = Long.parseLong(cfg.getApp().getOrDefault("measurementPeriodInSec", "30")) * 1000L;
        this.refreshDiscoveryPeriodInMs = Long.parseLong(cfg.getApp().getOrDefault("refreshDiscoveryPeriodInSec", "300")) * 1000L;

        this.services = Collections.emptyMap();
        this.monitors = Collections.emptyMap();
        this.metrics = Collections.emptyMap();
    }

    @Override
    public void run() {

        List<EVENT> evts = Arrays.asList(EVENT.UPDATE_TOPOLOGY, EVENT.POKE);
        EVENT evt;
        long start, stop;

        for (; ; ) {
            start = System.currentTimeMillis();
            evt = evts.get(0);
            dispatch_events(evt);
            stop = System.currentTimeMillis();
            logger.info("{} took {} ms", evt, stop - start);

            resheduleEvent(evt, start, stop);
            Collections.sort(evts, Comparator.comparingLong(event -> event.nexTick));

            try {
                long sleep_duration = evts.get(0).nexTick - System.currentTimeMillis() - 1;
                if (sleep_duration > 0) {
                    Thread.sleep(sleep_duration);
                    logger.info("WAIT took {} ms", sleep_duration);
                }
            } catch (InterruptedException e) {
                logger.error("thread interrupted {}", e);
            }
        }
    }

    private void resheduleEvent(EVENT lastEvt, long start, long stop) {
        long duration = stop - start;
        if (duration >= measurementPeriodInMs) {
            logger.warn("Operation took longer than 1 tick, please increase tick rate if you see this message too often");
        }

        switch (lastEvt) {
            case UPDATE_TOPOLOGY:
                lastEvt.nexTick = start + refreshDiscoveryPeriodInMs;
                break;

            case POKE:
                lastEvt.nexTick = start + measurementPeriodInMs;
                break;
        }
    }

    public void dispatch_events(EVENT evt) {
        switch (evt) {
            case UPDATE_TOPOLOGY:
                updateTopology();
                break;

            case POKE:
                poke();
                break;
        }
    }

    public void updateTopology() {
        final Map<Service, Set<InetSocketAddress>> new_services = discovery.getServicesNodesFor();

        // Discovery down?
        if (new_services.isEmpty()) {
            logger.warn("Discovery sent back no service to monitor. Is it down? Check your configuration.");
            return;
        }

        // Check if topology changed
        if (IDiscovery.areServicesEquals(services, new_services)) {
            logger.trace("No topology change.");
            return;
        }

        logger.info("Topology changed. Monitors are updating...");
        // Clean old monitors
        monitors.values().forEach(mo -> mo.ifPresent(MemcachedMonitor::close));
        metrics.values().forEach(MemcachedMetrics::close);

        // Create new ones
        services = new_services;
        final long timeoutInMs = cfg.getService().getTimeoutInSec() * 1000L;
        monitors = services.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> MemcachedMonitor.fromNodes(e.getKey(), e.getValue(),
                        timeoutInMs)
                ));

        metrics = new HashMap<>(monitors.size());
        for (Service service : monitors.keySet()) {
            metrics.put(service, new MemcachedMetrics(service));
        }
    }

    protected abstract void poke();

    @Override
    public void close() {
        discovery.close();
        monitors.values().forEach(mo -> mo.ifPresent(m -> {
            try {
                m.close();
            } catch (Exception e) {
                logger.error("Error when releasing resources", e);
            }
        }));
        metrics.values().forEach(MemcachedMetrics::close);
    }

    private enum EVENT {
        UPDATE_TOPOLOGY(System.currentTimeMillis()),
        POKE(System.currentTimeMillis());

        public long nexTick;

        EVENT(long nexTick) {
            this.nexTick = nexTick;
        }

    }
}
