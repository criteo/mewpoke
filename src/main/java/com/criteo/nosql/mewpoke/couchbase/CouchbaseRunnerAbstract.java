package com.criteo.nosql.mewpoke.couchbase;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;

import com.criteo.nosql.mewpoke.discovery.IDiscovery;
import com.criteo.nosql.mewpoke.discovery.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.criteo.nosql.mewpoke.config.Config;
import com.criteo.nosql.mewpoke.discovery.Consul;
import com.criteo.nosql.mewpoke.discovery.Dns;

public abstract class CouchbaseRunnerAbstract implements AutoCloseable, Runnable {

    private final Logger logger = LoggerFactory.getLogger(CouchbaseRunnerAbstract.class);

    protected final Config cfg;
    protected final long tickRate;
    private final IDiscovery discovery;

    protected Map<Service, Set<InetSocketAddress>> services;
    protected Map<Service, Optional<CouchbaseMonitor>> monitors;
    protected Map<Service, CouchbaseMetrics> metrics;
    long refreshConsulInMs;

    public CouchbaseRunnerAbstract(Config cfg) {
        this.cfg = cfg;
        this.discovery = buildDiscovery(cfg.getDiscovery());
        this.tickRate = Long.parseLong(cfg.getApp().getOrDefault("tickRateInSec", "20")) * 1000L;
        this.refreshConsulInMs = cfg.getDiscovery().getRefreshEveryMin() * 60 * 1000L;
        this.monitors = Collections.emptyMap();
        this.metrics = Collections.emptyMap();
        this.services = Collections.emptyMap();
    }

    //TODO: externalize in a Builder
    private IDiscovery buildDiscovery(Config.Discovery discovery) {
        Config.ConsulDiscovery consulCfg = discovery.getConsul();
        Config.StaticDiscovery staticCfg = discovery.getStaticDns();
        if (consulCfg != null) { //&& !consulCfg.isEmpty()
            logger.info("Consul configuration will be used");
            return new Consul(consulCfg.getHost(), consulCfg.getPort(),
                    consulCfg.getTimeoutInSec(), consulCfg.getReadConsistency(),
                    consulCfg.getTags());
        }
        if (staticCfg != null) {
            logger.info("Static configuration will be used");
            return new Dns(cfg.getService().getUsername(), cfg.getService().getPassword(), staticCfg.getHost(), staticCfg.getClustername());
        }
        logger.error("Bad configuration, no discovery provided");
        throw new RuntimeException("Bad configuration, no discovery provided"); //TODO: Should break the main loop here
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
        if (duration >= tickRate) {
            logger.warn("Operation took longer than 1 tick, please increase tick rate if you see this message too often");
        }

        EVENT.WAIT.nexTick = start + tickRate - 1;
        switch (lastEvt) {
            case WAIT:
                break;

            case UPDATE_TOPOLOGY:
                lastEvt.nexTick = start + refreshConsulInMs;
                break;

            case POKE:
                lastEvt.nexTick = start + tickRate;
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

                // Consul down ?
                if (new_services.isEmpty()) {
                    logger.info("Discovery sent back no services to monitor. is it down ? Are you sure of your tags ?");
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
    public void close() throws Exception {
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
