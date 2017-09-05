package edu.utexas.arlut.ciads.cpiGraph;

import com.tinkerpop.blueprints.Element;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;

public class CPIElement implements Element {
    protected CPIElement(String id, CPIGraph g) {
        this.id = id;
        this.graph = g;
    }
    @Override
    public <T> T getProperty(String key) {
        return null;
    }

    @Override
    public Set<String> getPropertyKeys() {
        return null;
    }

    @Override
    public void setProperty(String key, Object value) {

    }

    @Override
    public <T> T removeProperty(String key) {
        return null;
    }

    @Override
    public void remove() {

    }

    @Override
    public Object getId() {
        return id;
    }
    public static class Impl {
        Impl(String id) {
            properties = newHashMap();
            properties.put(CPIGraphFactory.ID, id);
        }
        Impl(Impl src) {
            properties = newHashMap(src.properties);
        }
        protected final Map<String, Object> properties;
    }
    protected final String id;
    protected final CPIGraph graph;
}
