package edu.utexas.arlut.ciads.cacheGraph;

import com.tinkerpop.blueprints.*;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

public class GraphTransaction implements TransactionalGraph, KeyIndexableGraph {

    GraphTransaction(CacheGraph cg) {
        graph = cg;
    }
    @Override
    public Features getFeatures() {
        return graph.getFeatures();
    }

    @Override
    public Vertex addVertex(Object id_) {
        String id = graph.vertexIdFactory.call();
        final CacheVertex cv = new CacheVertex(id, graph);
        addedVertices.put(id, cv);
        return cv;
    }

    @Override
    public Vertex getVertex(Object id) {
        // try, then fall through
        return null;
    }

    @Override
    public void removeVertex(Vertex v) {
        String id = v.getProperty(CacheGraph.ID);
        // remove edges
        for (Edge e : v.getEdges(Direction.BOTH)) {
            removeEdge(e);
        }

        addedVertices.remove(id);
        deletedVertices.add(id);
    }
    /*
    o remove edges
    o remove vertices
    o add vertices
    o add edges
     */

    @Override
    public Iterable<Vertex> getVertices() {
        return null;
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        return null;
    }

    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        return null;
    }

    @Override
    public Edge getEdge(Object id) {
        return null;
    }

    @Override
    public void removeEdge(Edge edge) {

    }

    @Override
    public Iterable<Edge> getEdges() {
        return null;
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        return null;
    }

    @Override
    public GraphQuery query() {
        return null;
    }
    // =======================================
    @Override
    public void shutdown() {

    }

    @Override
    public void commit() {

    }

    @Override
    public void rollback() {

    }
    @Override @Deprecated
    public void stopTransaction(Conclusion conclusion) {

    }
    // =======================================
    @Override
    public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {

    }

    @Override
    public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, Parameter... indexParameters) {

    }

    @Override
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        return null;
    }

    private final CacheGraph graph;

    final Map<String, CacheVertex> addedVertices = newHashMap();
    final Set<String> deletedVertices = newHashSet();

    final Map<String, CacheEdge> addedEdges = newHashMap();
    final Set<String> deletedEdges = newHashSet();


}
