package edu.utexas.arlut.ciads.cacheGraph;

import com.tinkerpop.blueprints.Element;

public abstract class WriteBehind {
    void shutdown() {}

    void addVertex(final CachedVertex proxy) {}

    void removeVertex(final CachedVertex proxy) {}

    void addEdge(final CachedEdge proxy) {}

    void removeEdge(final CachedEdge proxy) {}

    void setProperty(final CachedElement proxy, final String key, final Object val) {}

    void removeProperty(final CachedElement proxy, final String key) {}

    void addKeyIndex(final String key, Class<? extends Element> clazz) {}

    void dropKeyIndex(final String key, Class<? extends Element> clazz) {}
}
