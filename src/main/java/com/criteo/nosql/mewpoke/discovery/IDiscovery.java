package com.criteo.nosql.mewpoke.discovery;

import java.net.InetSocketAddress;
import java.util.*;

public interface IDiscovery extends AutoCloseable {

    Map<Service, Set<InetSocketAddress>> getServicesNodesFor();

    default void close() {
    }
}
