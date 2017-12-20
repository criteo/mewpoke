package com.criteo.nosql.mewpoke.discovery;

import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;

public class Dns implements IDiscovery {
    private static Logger logger = LoggerFactory.getLogger(Dns.class);

    private final String host;
    private final String username;
    private final String password;
    private final String clustername;

    public Dns(String username, String password, String host, String clusterName) {
        this.username = username;
        this.password = password;
        this.host = host;
        this.clustername = clusterName;
    }

    @Override
    public Map<Service, Set<InetSocketAddress>> getServicesNodesFor() {
        CouchbaseCluster couchbaseCluster = null;
        try {
            couchbaseCluster = CouchbaseCluster.create(host);
            final ClusterManager clusterManager = couchbaseCluster.clusterManager(username, password);
            final List<BucketSettings> buckets = clusterManager.getBuckets();
            final JsonArray clusterNodes = clusterManager.info().raw().getArray("nodes");
            final Map<Service, Set<InetSocketAddress>> servicesNodes = new HashMap<>();

            buckets.forEach(b -> {
                final String bucketname = b.name();
                final Service srv = new Service(clustername, bucketname);
                final Set<InetSocketAddress> nodes = new HashSet<>();

                clusterNodes.forEach(n -> {
                    final String ipaddr = ((JsonObject) n).getString("hostname").split(":")[0];
                    final Integer port = ((JsonObject) n).getObject("ports").getInt("direct");
                    logger.debug("Node {} for bucket {}", ipaddr, bucketname);
                    nodes.add(new InetSocketAddress(ipaddr, port));
                });
                servicesNodes.put(srv, nodes);
            });
            return servicesNodes;
        } catch (Exception e) {
            logger.error("Could not get Services for {}", host, e);
            return Collections.emptyMap();
        } finally {
            if (couchbaseCluster != null) {
                couchbaseCluster.disconnect();
            }
        }
    }
}
