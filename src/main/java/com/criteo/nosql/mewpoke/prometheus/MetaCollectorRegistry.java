package com.criteo.nosql.mewpoke.prometheus;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

// TODO: there is a bug when we define several collector with the same (what we want)
// TODO: So we cannot use this class as it.
public class MetaCollectorRegistry extends CollectorRegistry {
    public static final MetaCollectorRegistry metaRegistry = new MetaCollectorRegistry();
    private final CopyOnWriteArrayList<CollectorRegistry> registries = new CopyOnWriteArrayList<>();

    public MetaCollectorRegistry() {
    }

    @Override
    public void register(Collector m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unregister(Collector m) {
        throw new UnsupportedOperationException();
    }

    public void register(CollectorRegistry m) {
        registries.add(m);
    }

    public void unregister(CollectorRegistry m) {
        registries.remove(m);
    }

    @Override
    public void clear() {
        registries.clear();
    }

    @Override
    public Enumeration<Collector.MetricFamilySamples> metricFamilySamples() {
        return new MetricFamilySamplesEnumeration();
    }

    @Override
    public Enumeration<Collector.MetricFamilySamples> filteredMetricFamilySamples(Set<String> includedNames) {
        return new MetricFamilySamplesEnumeration();
    }

    @Override
    public Double getSampleValue(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Double getSampleValue(String name, String[] labelNames, String[] labelValues) {
        throw new UnsupportedOperationException();
    }

    class MetricFamilySamplesEnumeration implements Enumeration<Collector.MetricFamilySamples> {

        private final Iterator<CollectorRegistry> it = registries.iterator();
        private Enumeration<Collector.MetricFamilySamples> next = null;

        @Override
        public boolean hasMoreElements() {
            if (next != null && next.hasMoreElements()) {
                return true;
            }

            while (it.hasNext()) {
                next = it.next().metricFamilySamples();
                if (next.hasMoreElements()) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public Collector.MetricFamilySamples nextElement() {
            if (next != null && next.hasMoreElements()) {
                return next.nextElement();
            }

            while (it.hasNext()) {
                next = it.next().metricFamilySamples();
                if (next.hasMoreElements()) {
                    return next.nextElement();
                }
            }

            throw new NoSuchElementException();
        }
    }
}
