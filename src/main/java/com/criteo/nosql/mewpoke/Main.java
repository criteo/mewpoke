package com.criteo.nosql.mewpoke;

import com.criteo.nosql.mewpoke.config.Config;
import com.criteo.nosql.mewpoke.couchbase.CouchbaseRunnerLatency;
import com.criteo.nosql.mewpoke.couchbase.CouchbaseRunnerStats;
import com.criteo.nosql.mewpoke.memcached.MemcachedRunnerLatency;
import com.criteo.nosql.mewpoke.memcached.MemcachedRunnerStats;
import io.prometheus.client.exporter.HTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.logging.Level;


public class Main
{
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception
    {
        // Get the configuration
        final Config cfg;
        try
        {
            final String cfgPath = args.length > 0 ? args[0] : Config.DEFAULT_PATH;
            logger.info("Loading yaml config from {}", cfgPath);
            cfg = Config.fromFile(cfgPath);
            logger.trace("Loaded configuration: {}", cfg);
        }
        catch (Exception e)
        {
            logger.error("Cannot load config file", e);
            return;
        }

        // Start an http server to allow Prometheus scrapping
        final int httpServerPort = Integer.parseInt(cfg.getApp().getOrDefault("httpServerPort", "8080"));
        logger.info("Starting an http server on port {}", httpServerPort);
        final HTTPServer server = new HTTPServer(httpServerPort);

        // Get the runner depending on the configuration
        final String runnerType = cfg.getService().getType();
        final RUNNER runner = RUNNER.valueOf(runnerType);

        // Memcached client is too verbose, we keep only SEVERE messages
        java.util.logging.Logger memcachedLogger = java.util.logging.Logger.getLogger("net.spy.memcached");
        memcachedLogger.setLevel(Level.SEVERE);
        System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SunLogger");

        // If an unexpected exception occurs, we retry
        for (; ; )
        {
            try
            {
                logger.info("Run {}", runnerType);
                runner.run(cfg);
            }
            catch (Exception e)
            {
                logger.error("An unexpected exception was thrown", e);
            }
        }
    }

    private enum RUNNER
    {
        MEMCACHED(MemcachedRunnerLatency.class),
        MEMCACHED_STATS(MemcachedRunnerStats.class),
        COUCHBASE(CouchbaseRunnerLatency.class),
        COUCHBASE_STATS(CouchbaseRunnerStats.class);

        private final Class<? extends AutoCloseable> runner;

        RUNNER(Class<? extends AutoCloseable> runner)
        {
            this.runner = runner;
        }

        public void run(Config cfg)
        {
            try (AutoCloseable r = runner.getConstructor(Config.class).newInstance(cfg))
            {
                ((Runnable) r).run();
            }
            catch (Exception e)
            {
                logger.error("Can't instantiate runner for {}", runner.getCanonicalName(), e);
            }
        }
    }
}
