package edu.utexas.arlut.ciads.cacheGraph;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.tinkerpop.blueprints.util.ElementHelper.getProperties;

@Slf4j
public abstract class CachedElement implements Element {



    protected CachedElement(final String id,
                            final CachedGraph cg) {
        this.id = id;
        this.graph = cg;
        properties.put(IdGraph.ID, id);
    }
    protected CachedElement(final Element from,
                            final CachedGraph cg) {
        properties.putAll(getProperties(from));
        this.id = properties.get(CachedGraph.ID).toString();
        this.graph = cg;
    }

    Element getBase() {
        return baseElement;
    }
    void setBase(Element e) {
        baseElement = e;
    }

    @Override
    public <T> T getProperty(String key) {
        return (T) properties.get(key);
    }

    @Override
    public Set<String> getPropertyKeys() {
        return properties.keySet();
    }

    @Override
    public void setProperty(String key, Object value) {
        if (key.equals(CachedGraph.ID)) {
            throw new IllegalArgumentException("Unable to set value for reserved property " + IdGraph.ID);
        }
        properties.put(key, value);
        graph.writeBehind.setProperty(this, key, value);
    }

    @Override
    public <T> T removeProperty(String key) {
        if (key.equals(CachedGraph.ID)) {
            throw new IllegalArgumentException("Unable to remove value for reserved property " + IdGraph.ID);
        }
        graph.writeBehind.removeProperty(this, key);
        return (T) properties.remove(key);
    }
    @Override
    public Object getId() {
        return id;
    }

    // =======================================
    static class Impl {
        Impl() {
            properties = newHashMap();
        }
        Impl(Impl src) {
            properties = newHashMap(src.properties);
        }
        private final Map<String, Object> properties;
    }

    // =======================================
    // b/c we're write-behind, this may be slow in coming...
    protected Element baseElement = null;

    protected final CachedGraph graph;
    protected final String id;
    protected Map<String, Object> properties = newHashMap();
}
