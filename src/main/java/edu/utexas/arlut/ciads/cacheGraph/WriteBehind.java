package edu.utexas.arlut.ciads.cacheGraph;

import com.tinkerpop.blueprints.Element;

public abstract class WriteBehind {
    void shutdown() {}

    void addVertex(final CacheVertex proxy) {}

    void removeVertex(final CacheVertex proxy) {}

    void addEdge(final CacheEdge proxy) {}

    void removeEdge(final CacheEdge proxy) {}

    void setProperty(final CacheElement proxy, final String key, final Object val) {}

    void removeProperty(final CacheElement proxy, final String key) {}

    void addKeyIndex(final String key, Class<? extends Element> clazz) {}

    void dropKeyIndex(final String key, Class<? extends Element> clazz) {}
}
