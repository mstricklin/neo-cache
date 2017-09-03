package edu.utexas.arlut.ciads;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.*;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.PropertyFilteredIterable;
import com.tinkerpop.blueprints.util.StringFactory;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import static com.google.common.collect.Maps.newHashMap;
import static com.tinkerpop.blueprints.util.ElementHelper.copyProperties;
import static com.tinkerpop.blueprints.util.ElementHelper.getProperties;

@Slf4j
public class CacheGraph<T extends TransactionalGraph & KeyIndexableGraph>
        implements TransactionalGraph, KeyIndexableGraph {

    public static final String ID = "__id";
    private final T graph;
    final Cache<String, CacheVertex> vertexCache = CacheBuilder.newBuilder()
                                                          .maximumSize(1000)
                                                          .build();
    final Cache<String, CacheEdge> edgeCache = CacheBuilder.newBuilder()
                                                          .maximumSize(1000)
                                                          .build();

    @Setter
    private IdFactory vertexIdFactory;
    @Setter
    private IdFactory edgeIdFactory;
    public class NullWriteBehind extends WriteBehind {}
    WriteBehind writeBehind = new NullWriteBehind();


    CacheGraph(T graph) {
        this.graph = graph;
        vertexIdFactory = new DefaultIdFactory();
        edgeIdFactory = new DefaultIdFactory();

        vertexIndex.put(ID, new KeyIndex<>());
        edgeIndex.put(ID, new KeyIndex<>());

        // load with NullWriteBehind, so we don't write to a graph we're loading from
        initialLoad();
        writeBehind = new CacheWB<T>(graph);
    }

    private void initialLoad() {
        for (String ik: graph.getIndexedKeys(Vertex.class))
            createKeyIndex(ik, Vertex.class);
        for (String ik: graph.getIndexedKeys(Edge.class))
            createKeyIndex(ik, Edge.class);
        for (Vertex v: graph.getVertices())
            _addVertex(v);
        for (Edge e: graph.getEdges())
            _addEdge(e);
    }

    @Override
    public Features getFeatures() {
        return graph.getFeatures();
    }

    @Override
    public Vertex addVertex(Object id_) {
        String id = vertexIdFactory.call();
        final CacheVertex cv = new CacheVertex(id, this);
        vertexCache.put(id, cv);
        writeBehind.addVertex(cv);
        return cv;
    }
    private void _addVertex(Vertex v) {
        final CacheVertex cv = new CacheVertex(v, this);
        String id = v.getId().toString();
        vertexCache.put(id, cv);
    }

    @Override
    public Vertex getVertex(Object id) {
        return vertexCache.getIfPresent(id.toString());
    }

    @Override
    public void removeVertex(Vertex v) {
        String id = v.getProperty(ID);
        // remove edges
        for (Edge e: v.getEdges(Direction.BOTH)) {
            removeEdge(e);
        }
        vertexCache.invalidate(id);
        writeBehind.removeVertex((CacheVertex)v);
    }

    @Override
    public Iterable<Vertex> getVertices() {
        return ImmutableList.copyOf(vertexCache.asMap().values());
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        // TODO: look at indices...
        return new PropertyFilteredIterable<>(key, value, this.getVertices());
    }
    // =================================
    @Override
    public Edge addEdge(Object id_, Vertex outVertex, Vertex inVertex, String label) {
        String id = edgeIdFactory.call();
        CacheVertex oV = (CacheVertex)outVertex;
        CacheVertex iV = (CacheVertex)inVertex;

        final CacheEdge ce = new CacheEdge(id, oV, iV, label, this);
        oV.addOutEdge(ce);
        iV.addInEdge(ce);

        edgeCache.put(id, ce);
        writeBehind.addEdge(ce);
        return ce;
    }
    private CacheVertex resolve(Edge e, Direction d) {
        Vertex ov = e.getVertex(d);
        String ovid = ov.getProperty(ID).toString();
        return vertexCache.getIfPresent(ovid);
    }
    private void _addEdge(Edge e) {
        String id = e.getProperty(ID);

        CacheVertex oV = resolve(e, Direction.OUT);
        CacheVertex iV = resolve(e, Direction.IN);
        final CacheEdge ce = new CacheEdge(e, oV, iV, this);




//        final CacheEdge ce = new CacheEdge(id, this);
//        copyProperties(e, ce);
//        vertexCache.put(id, ce);
    }

    @Override
    public Edge getEdge(Object id) {
        return edgeCache.getIfPresent(id.toString());
    }

    @Override
    public void removeEdge(Edge e) {
        String id = e.getProperty(ID);
        edgeCache.invalidate(id);
        writeBehind.removeEdge((CacheEdge) e);
    }

    @Override
    public Iterable<Edge> getEdges() {
        return ImmutableList.copyOf(edgeCache.asMap().values());
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        // TODO: look at indices...
        return new PropertyFilteredIterable<>(key, value, this.getEdges());
    }
    // =================================
    @Override
    public GraphQuery query() {
        return null;
    }

    @Deprecated @Override
    public void stopTransaction(Conclusion conclusion) {

    }

    @Override
    public void shutdown() {
        log.info("CacheGraph shutdown");
        writeBehind.shutdown();
        graph.shutdown();
    }

    @Override
    public void commit() {

    }

    @Override
    public void rollback() {

    }
    // =================================
    // hashCode?
    public String toString() {
        return StringFactory.graphString(this, this.graph.toString());
    }
    // =================================

    @Override
    public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            vertexIndex.remove(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            edgeIndex.remove(key);
        }
        writeBehind.dropKeyIndex(key, elementClass);
    }

    @Override
    public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, Parameter... indexParameters) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            vertexIndex.put(key, new KeyIndex<>());
            writeBehind.addKeyIndex(key, elementClass);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            edgeIndex.put(key, new KeyIndex<>());
            writeBehind.addKeyIndex(key, elementClass);
        } else {
            throw ExceptionFactory.classIsNotIndexable(elementClass);
        }
    }

    @Override
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return vertexIndex.keySet();
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            return edgeIndex.keySet();
        }
        throw ExceptionFactory.classIsNotIndexable(elementClass);
    }

    // =================================
    public interface IdFactory extends Callable<String> {
        String call();
    }

    private static class DefaultIdFactory implements IdFactory {
        @Override
        public String call() {
            return UUID.randomUUID().toString();
        }
    }

    // [vertex|edge]:key:value:element
    Map<String, KeyIndex<Vertex>> vertexIndex = newHashMap();
    Map<String, KeyIndex<Edge>> edgeIndex = newHashMap();


    private static class KeyIndex<T extends Element> extends HashMap<Object, T> {

    }
}
