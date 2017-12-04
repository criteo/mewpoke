package com.criteo.nosql.mewpoke.discovery;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IDiscovery extends AutoCloseable {

    Map<Service, Set<InetSocketAddress>> getServicesNodesFor();

    void close();

    static boolean areServicesEquals(final Map<Service, Set<InetSocketAddress>> ori, Map<Service, Set<InetSocketAddress>> neo)
    {
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
}
