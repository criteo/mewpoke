package com.criteo.nosql.mewpoke.discovery;

import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.criteo.nosql.mewpoke.config.Config;
import com.ecwid.consul.v1.ConsistencyMode;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.catalog.CatalogClient;
import com.ecwid.consul.v1.catalog.CatalogConsulClient;
import com.ecwid.consul.v1.health.HealthClient;
import com.ecwid.consul.v1.health.HealthConsulClient;
import com.ecwid.consul.v1.health.model.HealthService;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * Provide a discovery based on Consul.
 */
public class ConsulDiscovery implements IDiscovery {
    private static final Logger logger = LoggerFactory.getLogger(ConsulDiscovery.class);

    private static final String MAINTENANCE_MODE = "_node_maintenance";

    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final int timeout;
    private final QueryParams params;
    private final List<String> tags;
    private final ExecutorService executor;

    public ConsulDiscovery(final Config.ConsulDiscovery consulCfg, String username, String password) {
        this.username = username;
        this.password = password;
        this.host = consulCfg.getHost();
        this.port = consulCfg.getPort();
        this.timeout = consulCfg.getTimeoutInSec();
        this.params = new QueryParams(ConsistencyMode.valueOf(consulCfg.getReadConsistency()));
        this.tags = consulCfg.getTags();
        this.executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("consul-%d").build());
    }

    private static String getFromTags(final HealthService.Service service, final String prefix) {
        return service.getTags().stream()
            .filter(tag -> tag.startsWith(prefix))
            .map(tag -> tag.substring(prefix.length()))
            .findFirst().orElse("NotDefined");
    }

    public static String getClusterName(final HealthService.Service service) {
        return getFromTags(service, "cluster-");
    }

    public static String getBucketName(final HealthService.Service service) {
        return getFromTags(service, "bucket-");
    }

    private Map<Service, Set<InetSocketAddress>> getServicesNodesByTags(final List<String> tags) {
        final CatalogClient client = new CatalogConsulClient(host, port);
        final List<String> serviceNames =
            client.getCatalogServices(params).getValue().entrySet().stream()
                .filter(entry -> !Collections.disjoint(entry.getValue(), tags))
                .map(Map.Entry::getKey)
                .collect(toList());
        return getServicesNodesByServices(serviceNames);
    }

    private Map<Service, Set<InetSocketAddress>> getServicesNodesByServices(final List<String> serviceNames) {
        final HealthClient client = new HealthConsulClient(host, port);
        final Map<Service, Set<InetSocketAddress>> servicesNodes = new HashMap<>(serviceNames.size());
        for (String serviceName : serviceNames) {
            final Set<InetSocketAddress> nodesFromConsul = new HashSet<>();
            final Set<InetSocketAddress> nodes = new HashSet<>();
            final Service[] srv = new Service[] {null};
            client.getHealthServices(serviceName, false, params).getValue().stream()
                // TODO: we ignore nodes when an health check message starts with 'DISCARD'. It allows to ignore spare nodes for couchbase. It's flaky but don't have better; come propose me better.
                // TODO: When consul starts, all health checks failed with no message for X seconds. We choose to ignore these nodes. But the test hardcode our convention; come propose me better.
                .filter(hsrv -> hsrv.getChecks().stream()
                    .noneMatch(check -> check.getCheckId().equalsIgnoreCase(MAINTENANCE_MODE)
                        || check.getOutput().startsWith("DISCARD:")
                        || (check.getCheckId().startsWith("service:couchbase") && check.getOutput().isEmpty())
                    ))
                .forEach(hsrv -> {
                    logger.debug("{}", hsrv.getNode());
                    String srvAddr = Strings.isNullOrEmpty(hsrv.getService().getAddress())
                        ? hsrv.getNode().getAddress()
                        : hsrv.getService().getAddress();
                    nodesFromConsul.add(new InetSocketAddress(srvAddr, hsrv.getService().getPort()));
                    srv[0] = new Service(getClusterName(hsrv.getService()), getBucketName(hsrv.getService()));
                });
            if (nodesFromConsul.size() > 0) {
                int port = nodesFromConsul.stream().findFirst().get().getPort();
                nodes.addAll(getNodesFromCouchbase(nodesFromConsul, port));
            }
            if (nodes.size() > 0) {
                logger.info("Found {} nodes for {}", nodes.size(), srv[0]);
                servicesNodes.put(srv[0], nodes);
            }
        }
        return servicesNodes;
    }

    private Set<InetSocketAddress> getNodesFromCouchbase(Set<InetSocketAddress> inetAddressConsulNodes, int port) {
        final Set<InetSocketAddress> nodes = new HashSet<>();
        CouchbaseCluster couchbaseCluster = null;
        try {
            List<String> consulNodes = inetAddressConsulNodes.stream()
                .map(inetAddressConsulNode -> inetAddressConsulNode.getAddress().getHostAddress()).collect(Collectors.toList());
            couchbaseCluster = CouchbaseCluster.create(consulNodes);
            final ClusterManager clusterManager = couchbaseCluster.clusterManager(username, password);
            final JsonArray clusterNodes = clusterManager.info().raw().getArray("nodes");
            clusterNodes.forEach(n -> {
                final String ipaddr = ((JsonObject) n).getString("hostname").split(":")[0];
                nodes.add(new InetSocketAddress(ipaddr, port));
            });
        }
        catch (Exception e) {
            logger.error("Could not get Services for {}", host, e);
            return Collections.emptySet();
        } finally {
            if (couchbaseCluster != null) {
                couchbaseCluster.disconnect();
            }
        }
        
        return nodes;
    }

    /**
     * Look in Consul for all services matching one of the tags
     * The function filter out nodes that are in maintenance mode
     *
     * @return the map nodes by services
     */
    // All this mumbo-jumbo with the executor is done only because the consul client does not expose
    // in any way a mean to timeout/cancel requests nor to properly shutdown/reset it.
    // Thus we play safe and wrap calls inside an executor that we can properly timeout, and a new consul client
    // is created each time.
    // TODO: Consider to have one shared ConsulRawClient, configured with a custom httpClient/requestConfig
    @Override
    public Map<Service, Set<InetSocketAddress>> getServicesNodes() {
        Future<Map<Service, Set<InetSocketAddress>>> fServices = null;
        try {
            fServices = executor.submit(() -> {
                logger.info("Fetching services for tag {} ", tags);
                final long start = System.currentTimeMillis();
                final Map<Service, Set<InetSocketAddress>> services = getServicesNodesByTags(tags);
                final long stop = System.currentTimeMillis();
                logger.info("Fetching services for tag {} took {} ms", tags, stop - start);
                return services;
            });
            return fServices.get(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Cannot fetch nodes for tag {}", tags, e);

            if (fServices != null) {
                fServices.cancel(true);
            }
            return Collections.emptyMap();
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
