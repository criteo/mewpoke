package com.criteo.nosql.mewpoke;

import com.criteo.nosql.mewpoke.config.Config;
import com.criteo.nosql.mewpoke.couchbase.CouchbaseRunnerLatency;
import com.criteo.nosql.mewpoke.couchbase.CouchbaseRunnerStats;
import com.criteo.nosql.mewpoke.discovery.ConsulDiscovery;
import com.criteo.nosql.mewpoke.discovery.CouchbaseDiscovery;
import com.criteo.nosql.mewpoke.discovery.IDiscovery;
import com.criteo.nosql.mewpoke.memcached.MemcachedRunnerLatency;
import com.criteo.nosql.mewpoke.memcached.MemcachedRunnerStats;
import com.criteo.nosql.mewpoke.memcached.MemcachedRunnerStatsItems;
import io.prometheus.client.exporter.HTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.logging.Level;


public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        // Get the configuration
        final Config cfg;
        try {
            final String cfgPath = args.length > 0 ? args[0] : Config.DEFAULT_PATH;
            logger.info("Loading yaml config from {}", cfgPath);
            cfg = Config.fromFile(cfgPath);
            logger.trace("Loaded configuration: {}", cfg);
        } catch (Exception e) {
            logger.error("Cannot load config file", e);
            return;
        }

        // Get the discovery
        final IDiscovery discovery = getDiscovery(cfg);

        // Start an http server to allow Prometheus scrapping
        // daemon=true, so if the scheduler is stopped, the JVM does not wait for http server termination
        final int httpServerPort = Integer.parseInt(cfg.getApp().getOrDefault("httpServerPort", "8080"));
        logger.info("Starting an http server on port {}", httpServerPort);
        final HTTPServer server = new HTTPServer(httpServerPort, true);

        // Memcached client is too verbose, we keep only SEVERE messages
        java.util.logging.Logger memcachedLogger = java.util.logging.Logger.getLogger("net.spy.memcached");
        memcachedLogger.setLevel(Level.SEVERE);
        System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SunLogger");

        // Get the runner depending on the configuration
        final String runnerType = cfg.getService().getType();

        // If an unexpected exception occurs, we retry
        for (; ; ) {
            logger.info("Initialise {}", runnerType);
            try(AutoCloseable runner = getRunner(runnerType, cfg, discovery)){
                try {
                    logger.info("Run {}", runnerType);
                    ((Runnable)runner).run();
                } catch (Exception e) {
                    logger.error("An unexpected exception was thrown", e);
                }

            }
        }
    }

    private static IDiscovery getDiscovery(Config cfg) {
        final Config.ConsulDiscovery consulCfg = cfg.getDiscovery().getConsul();
        final Config.StaticDiscovery staticCfg = cfg.getDiscovery().getStaticDns();
        if (consulCfg != null) {
            logger.info("Consul discovery will be used");
            return new ConsulDiscovery(consulCfg.getHost(), consulCfg.getPort(),
                    consulCfg.getTimeoutInSec(), consulCfg.getReadConsistency(),
                    consulCfg.getTags());
        }
        if (staticCfg != null) {
            logger.info("Static Couchbase discovery will be used");
            return new CouchbaseDiscovery(cfg.getService().getUsername(), cfg.getService().getPassword(), staticCfg.getHost(), staticCfg.getClustername());
        }
        logger.error("Bad configuration, no discovery was provided");
        throw new IllegalArgumentException("Bad configuration, no discovery was provided");
    }

    private static AutoCloseable getRunner(String type, Config cfg, IDiscovery discovery) {
        try {
            switch (type) {
                case "MEMCACHED":
                    return new MemcachedRunnerLatency(cfg, discovery);
                case "MEMCACHED_STATS":
                    return new MemcachedRunnerStats(cfg, discovery);
                case "MEMCACHED_SLABS":
                    return new MemcachedRunnerStatsItems(cfg, discovery);
                case "COUCHBASE":
                    return new CouchbaseRunnerLatency(cfg, discovery);
                case "COUCHBASE_STATS":
                    return new CouchbaseRunnerStats(cfg, discovery);
                default:
                    final Class clazz = Class.forName(type);
                    return (AutoCloseable) clazz
                            .getConstructor(Config.class, IDiscovery.class)
                            .newInstance(cfg, discovery);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot load the service " + type, e);
        }
    }
}
