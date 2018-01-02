package com.criteo.nosql.mewpoke.memcached;

import java.net.InetSocketAddress;
import java.util.*;

import com.criteo.nosql.mewpoke.discovery.IDiscovery;
import com.criteo.nosql.mewpoke.discovery.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.criteo.nosql.mewpoke.config.Config;

public abstract class MemcachedRunnerAbstract implements AutoCloseable, Runnable {

    private final Logger logger = LoggerFactory.getLogger(MemcachedRunnerAbstract.class);

    private final Config cfg;
    private final IDiscovery discovery;
    private final long measurementPeriodInMs;
    private final long refreshDiscoveryPeriodInMs;

    protected Map<Service, Set<InetSocketAddress>> services;
    protected final Map<Service, Optional<MemcachedMonitor>> monitors;
    protected final Map<Service, MemcachedMetrics> metrics;

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

        List<EVENT> evts = Arrays.asList(EVENT.UPDATE_TOPOLOGY, EVENT.WAIT, EVENT.POKE);
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
        }
    }

    private void resheduleEvent(EVENT lastEvt, long start, long stop) {
        long duration = stop - start;
        if (duration >= measurementPeriodInMs) {
            logger.warn("Operation took longer than 1 tick, please increase tick rate if you see this message too often");
        }

        EVENT.WAIT.nexTick = start + measurementPeriodInMs - 1;
        switch (lastEvt) {
            case WAIT:
                break;

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
            case WAIT:
                try {
                    Thread.sleep(Math.max(evt.nexTick - System.currentTimeMillis(), 0));
                } catch (Exception e) {
                    logger.error("thread interrupted {}", e);
                }
                break;

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
            logger.warn("Discovery sent back no services to monitor. Is it down? Check your configuration.");
            return;
        }

        // Dispose old monitors
        services.forEach((service, addresses) -> {
            if (!Objects.equals(addresses, new_services.get(service))) {
                logger.info("{} has changed, its monitor will be disposed.", service);
                monitors.remove(service)
                        .ifPresent(mon -> mon.close());
                metrics.remove(service)
                        .close();
            }
        });

        // Create new ones
        final long timeoutInMs = cfg.getService().getTimeoutInSec() * 1000L;

        new_services.forEach((service, new_addresses) -> {
            if (!Objects.equals(services.get(service), new_addresses)) {
                logger.info("A new Monitor for {} will be created.", service);
                monitors.put(service, MemcachedMonitor.fromNodes(service, new_addresses, timeoutInMs));
                metrics.put(service, new MemcachedMetrics(service));
            }
        });

        services = new_services;
    }

    protected abstract void poke();

    @Override
    public void close() {
        discovery.close();
        monitors.values().stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(mon -> mon.close());
        metrics.values()
                .forEach(metric -> metric.close());
    }

    private enum EVENT {
        UPDATE_TOPOLOGY(System.currentTimeMillis()),
        WAIT(System.currentTimeMillis()),
        POKE(System.currentTimeMillis());

        public long nexTick;

        EVENT(long nexTick) {
            this.nexTick = nexTick;
        }

    }
}
