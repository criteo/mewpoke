package com.criteo.nosql.mewpoke.couchbase;

import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.RequestHandler;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.message.config.RestApiResponse;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.api.ClusterApiClient;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.util.NodeLocatorHelper;
import com.criteo.nosql.mewpoke.config.Config;
import com.criteo.nosql.mewpoke.discovery.Service;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;


public class CouchbaseMonitor implements AutoCloseable {
    private static Logger logger = LoggerFactory.getLogger(CouchbaseMonitor.class);
    private static AtomicReference<CouchbaseEnvironment> couchbaseEnv = new AtomicReference<>(null);
    private final String serviceName;
    private final Config.CouchbaseStats couchbasestats;

    private final CouchbaseCluster client;
    private final long timeoutInMs;

    // To avoid too many allocation at each iteration we allocate buffers upfront
    // but as we share results with external world we have to guard against
    // someone mutating our internal buffers, so we freeze the map in order to share them safely
    private final Map<InetSocketAddress, Long> setLatencies;
    private final Map<InetSocketAddress, Long> setLatenciesFrozen;
    private final Map<InetSocketAddress, Boolean> availability;
    private final Map<InetSocketAddress, Boolean> availabilityFrozen;
    private final Map<InetSocketAddress, String> nodesMembership;
    private final Map<InetSocketAddress, String> nodesMembershipFrozen;
    private final Map<InetSocketAddress, Map<String, Double>> nodesApiStats;
    private final Map<InetSocketAddress, Map<String, Double>> nodesApiStatsFrozen;
    private final Map<String, Map<String, Double>> xdcrStats;
    private final Map<String, Map<String, Double>> xdcrStatsFrozen;
    private final Bucket bucket;
    private final RequestHandler requestHandler;
    private final Field nodesGetter;
    private final ArrayList<JsonDocument> docs;
    private final NodeLocatorHelper locator;
    private final ClusterManager clusterManager;

    private CouchbaseMonitor(String serviceName, CouchbaseCluster client, Bucket bucket, long timeoutInMs, String username, String password, Config.CouchbaseStats couchbasestats) throws NoSuchFieldException, IllegalAccessException {
        this.serviceName = serviceName;
        this.client = client;
        this.bucket = bucket;
        this.clusterManager = client.clusterManager(username, password);
        this.timeoutInMs = timeoutInMs;
        this.couchbasestats = couchbasestats;

        final CouchbaseCore c = ((CouchbaseCore) bucket.core());

        final Field f = c.getClass().getDeclaredField("requestHandler");
        f.setAccessible(true);
        this.requestHandler = (RequestHandler) f.get(c);
        this.nodesGetter = requestHandler.getClass().getDeclaredField("nodes");
        nodesGetter.setAccessible(true);

        this.setLatencies = new HashMap<>(getNodes().size());
        this.availability = new HashMap<>(getNodes().size());
        this.nodesMembership = new HashMap<>(getNodes().size());
        this.nodesApiStats = new HashMap<>(getNodes().size());
        this.xdcrStats = new HashMap<>();
        this.setLatenciesFrozen = Collections.unmodifiableMap(this.setLatencies);
        this.availabilityFrozen = Collections.unmodifiableMap(this.availability);
        this.nodesMembershipFrozen = Collections.unmodifiableMap(this.nodesMembership);
        this.nodesApiStatsFrozen = Collections.unmodifiableMap(this.nodesApiStats);
        this.xdcrStatsFrozen = Collections.unmodifiableMap(this.xdcrStats);

        // Generate requests that will spread on every nodes
        this.locator = NodeLocatorHelper.create(bucket);
        final Map<InetAddress, String> keysHolder = new HashMap<>(locator.nodes().size());
        for (int i = 0; keysHolder.size() < locator.nodes().size() && i < 2000; i++) {
            final String key = "mewpoke_" + i;
            keysHolder.put(locator.activeNodeForId(key), key);
        }

        this.docs = new ArrayList<>(keysHolder.size());
        for (String key : keysHolder.values()) {
            this.docs.add(JsonDocument.create(key, null, -1));
        }
    }

    private static InetSocketAddress nodeToInetAddress(Node n) {
        return new InetSocketAddress(n.hostname().hostname(), 8091);
    }

    public static Optional<CouchbaseMonitor> fromNodes(final Service service, Set<InetSocketAddress> endPoints, Config config) {
        if (endPoints.isEmpty()) {
            return Optional.empty();
        }

        final Config.CouchbaseStats bucketStats = config.getCouchbaseStats();
        final String bucketpassword = config.getService().getBucketpassword();
        final long timeoutInMs = config.getService().getTimeoutInSec() * 1000L;
        final String username = config.getService().getUsername();
        final String password = config.getService().getPassword();
        final String bucketName = service.getBucketName();

        CouchbaseCluster client = null;
        Bucket bucket = null;
        try {
            final CouchbaseEnvironment env = couchbaseEnv.updateAndGet(e -> e == null ? DefaultCouchbaseEnvironment.builder().retryStrategy(FailFastRetryStrategy.INSTANCE).build() : e);
            client = CouchbaseCluster.create(env, endPoints.stream().map(e -> e.getHostString()).collect(Collectors.toList()));
            bucket = client.openBucket(bucketName,bucketpassword);
            return Optional.of(new CouchbaseMonitor(bucketName, client, bucket, timeoutInMs, username, password, bucketStats));
        } catch (Exception e) {
            logger.error("Cannot create couchbase client for {}", bucketName, e);
            if (client != null) client.disconnect();
            if (bucket != null) bucket.close();
            return Optional.empty();
        }
    }

    private CopyOnWriteArrayList<Node> getNodes() {
        try {
            return (CopyOnWriteArrayList<Node>) nodesGetter.get(requestHandler);
        } catch (IllegalAccessException e) {
            return new CopyOnWriteArrayList<>();
        }
    }

    public boolean collectRebalanceOps() {
        try {
            return this.clusterManager.info().raw().getString("rebalanceStatus").equalsIgnoreCase("running");
        } catch (Exception e) {
            logger.error("Got an invalid JSON from the couchbase API for {} on rebalanceOps", serviceName, e);
            return false;
        }
    }

    public Map<InetSocketAddress, String> collectMembership() {
        try {
            final JsonArray clusterNodesStats = this.clusterManager.info().raw().getArray("nodes");
            clusterNodesStats.forEach(n -> {
                final JsonObject node = ((JsonObject) n);
                String hostname = node.getString("hostname");
                nodesMembership.put(new InetSocketAddress(hostname.substring(0, hostname.indexOf(':')), 8091), node.getString("clusterMembership"));
            });

            return nodesMembershipFrozen;
        } catch (Exception e) {
            logger.error("Got an invalid JSON from the couchbase API for {} on membership", serviceName, e);
            return Collections.emptyMap();
        }
    }

    public Map<InetSocketAddress, Map<String, Double>> collectApiStatsBucket() {
        final ClusterApiClient api = this.clusterManager.apiClient();

        for (Node n : getNodes()) {
            final String ipaddr = nodeToInetAddress(n).toString().split("/")[1];
            final ObjectMapper objectMapper = new ObjectMapper();

            final String uri = "/pools/default/buckets/" + this.bucket.name() + "/nodes/" + ipaddr + "/stats";
            final JsonNode jsonBucketStatsNode;
            try {
                final RestApiResponse BucketStatsNode = api.get(uri).execute();
                if (BucketStatsNode.httpStatus().code() != 200) {
                    logger.error("uri {} does not return 200", uri);
                    continue;
                }

                jsonBucketStatsNode = objectMapper.readTree(BucketStatsNode.body());
            } catch (Exception e) {
                logger.error("Cannot get stats from {}", uri, e);
                continue;
            }

            final Map<String, Double> MapStr = new HashMap<>();
            final JsonNode hot_keys = jsonBucketStatsNode.findValue("hot_keys");
            for (int i = 0; i < Math.min(hot_keys.size(), 3); i++) {
                final String key = "hot_keys." + i;
                try {
                    final JsonNode hot_key = hot_keys.get(i);
                    final double value = hot_key.get("ops").asDouble();
                    MapStr.put(key, value);
                } catch (Exception e) {
                    logger.error("Cannot find a metric convertible to double value for {}, stat {}", ipaddr, key, e);
                }
            }

            // We extract all configured stats, or ALL stats.
            if (couchbasestats.getBucket() == null) {
                final Iterator<Map.Entry<String, JsonNode>> fields = jsonBucketStatsNode.get("op").get("samples").fields();
                while (fields.hasNext()) {
                    final Map.Entry<String, JsonNode> field = fields.next();
                    try {
                        final double value = field.getValue().get(0).asDouble();
                        MapStr.put(field.getKey(), value);
                    } catch (Exception e) {
                        logger.error("Cannot find a metric convertible to double value for {}, stat {}", ipaddr, field.getKey(), e);
                    }
                }
            } else {
                for (String statname : couchbasestats.getBucket()) {
                    try {
                        final double value = jsonBucketStatsNode.findValue(statname).get(0).asDouble();
                        MapStr.put(statname, value);
                    } catch (Exception e) {
                        logger.error("Cannot find a metric convertible to double value for {}, stat {}", ipaddr, statname, e);
                    }
                }
            }
            nodesApiStats.put(nodeToInetAddress(n), MapStr);
        }
        return nodesApiStatsFrozen;
    }

    public Map<String, Map<String, Double>> collectApiStatsBucketXdcr() {
        final ClusterApiClient api = this.clusterManager.apiClient();
        final ObjectMapper objectMapper = new ObjectMapper();
        final String xdcrUri = "/pools/default/buckets/@xdcr-" + this.bucket.name() + "/stats";
        final JsonNode jsonBucketStatsXdcr;
        try {
            final RestApiResponse bucketStatsXdcr = api.get(xdcrUri).execute();
            if (bucketStatsXdcr.httpStatus().code() != 200) {
                logger.error("uri {} does not return 200", xdcrUri);
                return Collections.emptyMap();
            }
            jsonBucketStatsXdcr = objectMapper.readTree(bucketStatsXdcr.body());
        } catch (Exception e) {
            logger.error("Cannot get xdcr stats from {}", xdcrUri, e);
            return Collections.emptyMap();
        }
        if (jsonBucketStatsXdcr.findValue("timestamp").size() == 0) {
            return Collections.emptyMap();
        }

        final String remoteClustersUri = "/pools/default/remoteClusters";
        final JsonNode jsonRemoteCluster;
        try {
            final RestApiResponse remoteCluster = api.get(remoteClustersUri).execute();
            if (remoteCluster.httpStatus().code() != 200) {
                logger.error("uri {} does not return 200", remoteClustersUri);
                return Collections.emptyMap();
            }
            jsonRemoteCluster = objectMapper.readTree(remoteCluster.body());
        } catch (Exception e) {
            logger.error("Cannot get remote clusters from {}", remoteClustersUri, e);
            return Collections.emptyMap();
        }

        for (int i = 0; i < jsonRemoteCluster.size(); i++) { //We suppose bucket from remote and local cluster have same name
            final String remoteClusterName = jsonRemoteCluster.get(i).findValue("name").asText();
            // We assume that XDRC replication are between buckets with the same name
            final String remoteClusterUuid = jsonRemoteCluster.get(i).findValue("uuid").asText();
            final String srcBucketName = this.bucket.name();
            final String dstBucketName = this.bucket.name();
            final String xdcrStatPrefix = "replications/" + remoteClusterUuid + "/" + srcBucketName + "/" + dstBucketName + "/";
            final Map<String, Double> mapStats = new HashMap<>();
            if (couchbasestats.getXdcr() == null) {
                final Iterator<Map.Entry<String, JsonNode>> fields = jsonBucketStatsXdcr.get("op").get("samples").fields();
                while (fields.hasNext()) {
                    final Map.Entry<String, JsonNode> field = fields.next();
                    try {
                        final String statname = field.getKey().substring(field.getKey().lastIndexOf('/') + 1);
                        final double value = field.getValue().get(0).asDouble();
                        mapStats.put(statname, value);
                    } catch (Exception e) {
                        logger.error("Cannot find an XDCR metric convertible to double value for bucket {}, stat {}", srcBucketName, field.getKey(), e);
                    }
                }
            } else {
                for (String statname : couchbasestats.getXdcr()) {
                    try {
                        final double value = jsonBucketStatsXdcr.findValue(xdcrStatPrefix + statname).get(0).asDouble();
                        mapStats.put(statname, value);
                    } catch (Exception e) {
                        logger.error("Cannot find an XDCR metric convertible to double value for bucket {}, stat {}", srcBucketName, statname, e);
                    }
                }
            }
            xdcrStats.put(remoteClusterName, mapStats);
        }
        return xdcrStatsFrozen;
    }

    public Map<InetSocketAddress, Boolean> collectAvailability() {
        for (Node n : getNodes()) {
            availability.put(nodeToInetAddress(n), n.state() == LifecycleState.CONNECTED);
        }
        return availabilityFrozen;
    }

    public Map<InetSocketAddress, Long> collectLatencies(PersistTo persistTo, ReplicateTo replicateTo) {
        Observable
                .from(docs)
                .flatMap(doc -> {
                    final long start = System.nanoTime();
                    Tuple2<Observable<Document>, UpsertRequest> ret = bucket.async().upsertWithRequest(doc, persistTo, replicateTo);
                    return ret.value1()
                            .timeout(timeoutInMs, TimeUnit.MILLISECONDS)
                            .onErrorReturn(e -> null)
                            .doOnError(e -> setLatencies.put(nodeToInetAddress(ret.value2().node), timeoutInMs))
                            .doOnCompleted(() -> {
                                long stop = System.nanoTime();
                                setLatencies.put(nodeToInetAddress(ret.value2().node), (stop - start) / 1000L);
                            });
                })
                .last()
                .toBlocking()
                .firstOrDefault(null);

        return setLatenciesFrozen;
    }

    public Map<InetSocketAddress, Long> collectPersistToDiskLatencies() {
        return collectLatencies(PersistTo.MASTER, ReplicateTo.NONE);
    }

    @Override
    public void close() {
        try {
            bucket.close();
        } catch (Exception e) {
            logger.error("Cannot close bucket properly for {} ", serviceName, e);
        }
        try {
            client.disconnect();
        } catch (Exception e) {
            logger.error("Cannot disconnect couchbase client properly for {}", serviceName, e);
        }
    }
}
