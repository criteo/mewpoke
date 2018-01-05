package com.criteo.nosql.mewpoke.couchbase;

import com.criteo.nosql.mewpoke.config.Config;
import com.criteo.nosql.mewpoke.discovery.IDiscovery;
import com.criteo.nosql.mewpoke.discovery.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;

public abstract class CouchbaseRunnerAbstract implements AutoCloseable, Runnable {

    private final Logger logger = LoggerFactory.getLogger(CouchbaseRunnerAbstract.class);

    private final Config cfg;
    private final IDiscovery discovery;
    private final long measurementPeriodInMs;
    private final long refreshDiscoveryPeriodInMs;

    protected Map<Service, Set<InetSocketAddress>> services;
    protected final Map<Service, Optional<CouchbaseMonitor>> monitors;
    protected final Map<Service, CouchbaseMetrics> metrics;

    public CouchbaseRunnerAbstract(Config cfg, IDiscovery discovery) {
        this.cfg = cfg;
        this.discovery = discovery;
        this.measurementPeriodInMs = Long.parseLong(cfg.getApp().getOrDefault("measurementPeriodInSec", "30")) * 1000L;
        this.refreshDiscoveryPeriodInMs = Long.parseLong(cfg.getApp().getOrDefault("refreshDiscoveryPeriodInSec", "300")) * 1000L;

        this.services = Collections.emptyMap();
        this.monitors = new HashMap<>();
        this.metrics = new HashMap<>();
    }

    /**
     * Run monitors and discovery periodically.
     * It is an infinite loop. We can stop by interrupting its Thread
     */
    @Override
    public void run() {

        final List<EVENT> evts = Arrays.asList(EVENT.UPDATE_TOPOLOGY, EVENT.POKE);

        try {
            for (; ; ) {
                final long start = System.currentTimeMillis();
                final EVENT evt = evts.get(0);
                dispatch_events(evt);
                final long stop = System.currentTimeMillis();
                logger.info("{} took {} ms", evt, stop - start);

                resheduleEvent(evt, start, stop);
                Collections.sort(evts, Comparator.comparingLong(event -> event.nexTick));

                final long sleep_duration = evts.get(0).nexTick - System.currentTimeMillis() - 1;
                if (sleep_duration > 0) {
                    Thread.sleep(sleep_duration);
                    logger.info("WAIT took {} ms", sleep_duration);
                }
            }
        } catch (InterruptedException e) {
            logger.error("The run was interrupted");
            Thread.currentThread().interrupt();
        }
    }

    private void resheduleEvent(EVENT lastEvt, long start, long stop) {
        final long duration = stop - start;
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
        final String username = cfg.getService().getUsername();
        final String password = cfg.getService().getPassword();
        final Config.CouchbaseStats cbStats = cfg.getCouchbaseStats();
        new_services.forEach((service, new_addresses) -> {
            if (!Objects.equals(services.get(service), new_addresses)) {
                logger.info("A new Monitor for {} will be created.", service);
                monitors.put(service, CouchbaseMonitor.fromNodes(service, new_addresses, timeoutInMs,
                        username, password, cbStats));
                metrics.put(service, new CouchbaseMetrics(service));
            }
        });

        services = new_services;
    }

    protected abstract void poke();

    @Override
    public void close() {
        discovery.close();
        monitors.values().forEach(mo -> mo.ifPresent(m ->
                m.close()
        ));
        metrics.values().forEach(CouchbaseMetrics::close);
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
