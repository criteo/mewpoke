package com.criteo.nosql.mewpoke.discovery;

import java.net.InetSocketAddress;
import java.util.*;

public interface IDiscovery extends AutoCloseable {

    Map<Service, Set<InetSocketAddress>> getServicesNodesFor();

    default void close() {
    }

    static boolean areServicesEquals(final Map<Service, Set<InetSocketAddress>> ori, Map<Service, Set<InetSocketAddress>> neo) {
        if (ori.size() != neo.size()) {
            return false;
        }

        for (Map.Entry<Service, Set<InetSocketAddress>> e : ori.entrySet()) {
            final Set<InetSocketAddress> oriAddresses = e.getValue();
            final Set<InetSocketAddress> neoAddresses = neo.get(e.getKey());
            if (!Objects.equals(oriAddresses, neoAddresses)) {
                return false;
            }
        }

        return true;
    }
}
