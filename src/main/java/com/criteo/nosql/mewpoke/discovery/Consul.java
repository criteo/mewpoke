package com.criteo.nosql.mewpoke.discovery;

import com.ecwid.consul.v1.ConsistencyMode;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.health.model.HealthService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;

public class Consul implements IDiscovery
{
    private static final Logger logger = LoggerFactory.getLogger(Consul.class);
    private static final String MAINTENANCE_MODE = "_node_maintenance";

    private final String host;
    private final int port;
    private final int timeout;
    private final QueryParams params;
    private final List<String> tags;
    private final ExecutorService executor;

    public Consul(final String host, final int port, final int timeout, final String readConsistency, final List<String> tags)
    {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.params = new QueryParams(ConsistencyMode.valueOf(readConsistency));
        this.tags = tags;
        this.executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("consul-%d").build());
    }

    public static boolean areServicesEquals(final Map<Service, Set<InetSocketAddress>> ori, Map<Service, Set<InetSocketAddress>> neo)
    {
        //TODO Improve complexity of the function
        if (ori.size() != neo.size()) {
            return false;
        }

        for(Map.Entry<Service, Set<InetSocketAddress>> e: ori.entrySet()) {
           Set<InetSocketAddress> addresses = neo.getOrDefault(e.getKey(), Collections.emptySet());
           if (addresses.size() != e.getValue().size() || !e.getValue().containsAll(addresses)) {
               return false;
           }
        }

        return true;
    }

    private static String getFromTags(final HealthService.Service service, final String prefix){
        return service.getTags().stream()
                .filter(tag -> tag.startsWith(prefix))
                .map(tag -> tag.substring(prefix.length()))
                .findFirst().orElse("NotDefined");
    }

    public static String getClusterName(final HealthService.Service service)
    {
        return getFromTags(service, "cluster=");
    }

    public static String getBucketName(final HealthService.Service service)
    {
        return getFromTags(service, "bucket=");
    }

    private Map<Service, Set<InetSocketAddress>> getServicesNodesForImpl(List<String> tags)
    {
        ConsulClient client = new ConsulClient(host, port);
        final String[] platformSuffixes = new String[] {"-eu", "-us", "-as"};

        List<Map.Entry<String, List<String>>> services =
        client.getCatalogServices(params).getValue().entrySet().stream()
              .filter(entry -> !Collections.disjoint(entry.getValue(), tags)
                               && Arrays.stream(platformSuffixes).noneMatch(suffix -> entry.getKey().toLowerCase().endsWith(suffix)))
              .map(entry -> {
                  logger.info("Found service matching {}", entry.getKey());
                  return entry;
              })
              .collect(toList());

        Map<Service, Set<InetSocketAddress>> servicesNodes = new HashMap<>(services.size());
        for (Map.Entry<String, List<String>> service : services)
        {
            final Set<InetSocketAddress> nodes = new HashSet<>();
            final Service[] srv = new Service[] { null };
            client.getHealthServices(service.getKey(), false, params).getValue().stream()
                  .filter(hsrv -> hsrv.getChecks().stream()
                                    .noneMatch(check -> check.getCheckId().equalsIgnoreCase(MAINTENANCE_MODE)
                                                        || check.getOutput().startsWith("DISCARD:") // For couchbase, flaky but don't have better, come propose me better
                                                        || (check.getCheckId().startsWith("service:couchbase") && check.getOutput().isEmpty())
                                    ))
                  .forEach(hsrv -> {
                      logger.debug("{}", hsrv.getNode());
                      nodes.add(new InetSocketAddress(hsrv.getNode().getAddress(), hsrv.getService().getPort()));
                      srv[0] = new Service(Consul.getClusterName(hsrv.getService()), Consul.getBucketName(hsrv.getService()));
                  });
            if (nodes.size() > 0)
            {
                servicesNodes.put(srv[0], nodes);
            }
        }
        return servicesNodes;

    }

    /**
     * Entry point of the class, that fetch all services with associated nodes matching the given tag
     * The function filter out nodes that are in maintenance mode
     *
     * @return All this mumbo-jumbo with the executor is done only because the consul client does not expose
     * in any way a mean to timeout/cancel requests nor to properly shutdown/reset it.
     * Thus we play safe and wrap calls inside an executor that we can properly timeout, and a new consul client
     * is created each time.
     */
    @Override
    public Map<Service, Set<InetSocketAddress>> getServicesNodesFor()
    {
        Future<Map<Service, Set<InetSocketAddress>>> fServices = null;
        try
        {
            fServices = executor.submit(() -> {
                logger.info("Fetching services for tag {} ", tags);
                long start = System.currentTimeMillis();

                Map<Service, Set<InetSocketAddress>> services = getServicesNodesForImpl(tags);

                long stop = System.currentTimeMillis();
                logger.info("Fetching services for tag {} took {} ms", tags, stop - start);
                return services;
            });
            return fServices.get(timeout, TimeUnit.SECONDS);
        }
        catch (Exception e)
        {
            logger.error("Cannot fetch nodes for tag {}", tags, e);

            if (fServices != null)
            {
                fServices.cancel(true);
            }
            return Collections.emptyMap();
        }
    }

    @Override
    public void close()
    {
        executor.shutdownNow();
    }
}
