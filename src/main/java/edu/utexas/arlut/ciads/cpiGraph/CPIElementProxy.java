package edu.utexas.arlut.ciads.cpiGraph;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.ElementHelper;
import lombok.Getter;
import lombok.Setter;

public abstract class CPIElementProxy implements Element {
    public static Function<CPIElement, String> ID() {
        return new Function<CPIElement, String>() {
            @Override
            public String apply(CPIElement e) {
                return e.id;
            }
        };
    }
    protected CPIElementProxy(String id, CPIGraph g) {
        this.id = id;
        this.graph = g;
    }
    abstract CPIElement getImpl();
    abstract CPIElement getMutableImpl();
    @Override
    public <T> T getProperty(String key) {
        return (T)getImpl().properties.get(key);
    }

    @Override
    public Set<String> getPropertyKeys() {
        Set<String> keys = newHashSet(getImpl().properties.keySet());
        keys.remove(CPIGraph.ID);
        return keys;
    }

    @Override
    public void setProperty(String key, Object value) {
        ElementHelper.validateProperty(this, key, value);
        CPIElement impl = getMutableImpl();
        impl.properties.put(key, value);
        graph.manager.setProperty(impl, key, value);
    }


    @Override
    public <T> T removeProperty(String key) {
        CPIElement impl = getMutableImpl();
        T t = (T)impl.properties.remove(key);
        graph.manager.removeProperty(impl, key);
        return t;
    }

    @Override
    abstract public void remove();

    @Override
    public Object getId() {
        return id;
    }
    public String rawId() {
        return id;
    }
    // =================================
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
    // =================================
    public abstract static class CPIElement {
        CPIElement(String id) {
            this.id = id;
            properties = newHashMap();
        }
        CPIElement(CPIElement src) {
            this.id = src.id;
            properties = newHashMap(src.properties);
        }
        CPIElement(String id, Element src) {
            this(id);
            putProperties(src);
        }
        void putProperties(Element e) {
            for (String key: e.getPropertyKeys()) {
                properties.put(key, e.getProperty(key));
            }
        }
        abstract Element getBase();
        @Getter
        protected final String id;
        protected final Map<String, Object> properties;
    }
    protected final String id;
    protected final CPIGraph graph;
}
