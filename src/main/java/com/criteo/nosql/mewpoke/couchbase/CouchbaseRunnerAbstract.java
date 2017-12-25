package com.criteo.nosql.mewpoke.couchbase;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;

import com.criteo.nosql.mewpoke.discovery.IDiscovery;
import com.criteo.nosql.mewpoke.discovery.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.criteo.nosql.mewpoke.config.Config;
import com.criteo.nosql.mewpoke.discovery.ConsulDiscovery;
import com.criteo.nosql.mewpoke.discovery.CouchbaseDiscovery;

public abstract class CouchbaseRunnerAbstract implements AutoCloseable, Runnable {

    private final Logger logger = LoggerFactory.getLogger(CouchbaseRunnerAbstract.class);

    private final Config cfg;
    private final IDiscovery discovery;
    private final long measurementPeriodInMs;
    private final long refreshDiscoveryPeriodInMs;

    protected Map<Service, Set<InetSocketAddress>> services;
    protected Map<Service, Optional<CouchbaseMonitor>> monitors;
    protected Map<Service, CouchbaseMetrics> metrics;

    public CouchbaseRunnerAbstract(Config cfg, IDiscovery discovery) {
        this.cfg = cfg;
        this.discovery = discovery;
        this.measurementPeriodInMs = Long.parseLong(cfg.getApp().getOrDefault("measurementPeriodInSec", "30")) * 1000L;
        this.refreshDiscoveryPeriodInMs = Long.parseLong(cfg.getApp().getOrDefault("refreshDiscoveryPeriodInSec", "300")) * 1000L;

        this.monitors = Collections.emptyMap();
        this.metrics = Collections.emptyMap();
        this.services = Collections.emptyMap();
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
                Map<Service, Set<InetSocketAddress>> new_services = discovery.getServicesNodesFor();

                // Discovery down ?
                if (new_services.isEmpty()) {
                    logger.info("Discovery sent back no services to monitor. Is it down? Check your configuration.");
                    break;
                }

                // Check if topology has changed
                if (IDiscovery.areServicesEquals(services, new_services))
                    break;

                logger.info("Topology changed, updating it");
                // Clean old monitors
                monitors.values().forEach(mo -> mo.ifPresent(CouchbaseMonitor::close));
                metrics.values().forEach(CouchbaseMetrics::close);

                // Create new ones
                services = new_services;
                monitors = services.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> CouchbaseMonitor.fromNodes(e.getKey(), e.getValue(),
                                cfg.getService().getTimeoutInSec() * 1000L,
                                cfg.getService().getUsername(),
                                cfg.getService().getPassword(),
                                cfg.getCouchbaseStats())
                        ));

                metrics = new HashMap<>(monitors.size());
                for (Map.Entry<Service, Optional<CouchbaseMonitor>> client : monitors.entrySet()) {
                    metrics.put(client.getKey(), new CouchbaseMetrics(client.getKey()));
                }
                break;

            case POKE:
                poke();
                break;
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
        metrics.values().forEach(CouchbaseMetrics::close);
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
