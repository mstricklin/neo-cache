package edu.utexas.arlut.ciads.cpiGraph;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.FluentIterable;
import com.tinkerpop.blueprints.*;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

public class CPIGraph implements KeyIndexableGraph, TransactionalGraph {

    CPIGraph(String graphId, CPIGraphFactory factory) {

        this.graphId = graphId;
        this.factory = factory;
    }

    @Override
    public Features getFeatures() {
        return null;
    }

    @Override
    public Vertex addVertex(Object id_) {
        String id = factory.vertexId();
        CPIVertex.Impl impl = new CPIVertex.Impl(id);
        mutatedVertices.put(id, impl);
        // TODO: write-behind action add vertex
        // TODO: write-behind action setProperty ID
        return new CPIVertex(id, this);
    }

    @Override
    public Vertex getVertex(Object id) {
        if (deletedVertices.contains(id))
            return null;
        if (mutatedVertices.containsKey(id.toString())
                || (null != vertexCache.getIfPresent(id.toString())))
            return new CPIVertex(id.toString(), this);
        return null;
    }

    @Override
    public void removeVertex(Vertex vertex) {
        String id = vertex.getId().toString();
        mutatedVertices.remove(id);
        deletedVertices.add(id);
        // TODO: write-behind action remove vertex
    }



    @Override
    public Iterable<Vertex> getVertices() {
        return FluentIterable.from(vertexCache.asMap().keySet())
                .append(mutatedVertices.keySet())
                .filter(not(in(deletedVertices)))
                .transform(CPIVertex.MAKE(this))
                .transform(CPIVertex.DOWNCAST);
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        return null;
    }

    @Override
    public Edge addEdge(Object id_, Vertex outVertex, Vertex inVertex, String label) {
        // TODO: assert non-null & cast-ability
        String id = factory.edgeId();
        CPIEdge.Impl impl = new CPIEdge.Impl(id, outVertex.getId().toString(), inVertex.getId().toString(), label);
        mutatedEdges.put(id, impl);
        // TODO: write-behind action add edge
        // TODO: write-behind action setProperty ID
        return new CPIEdge(id, this);
    }

    @Override
    public Edge getEdge(Object id) {
        if (deletedEdges.contains(id))
            return null;
        if (mutatedEdges.containsKey(id.toString())
                || (null != edgeCache.getIfPresent(id.toString())))
            return new CPIEdge(id.toString(), this);
        return null;
    }

    @Override
    public void removeEdge(Edge edge) {
        String id = edge.getId().toString();
        mutatedEdges.remove(id);
        deletedEdges.add(id);
        // TODO: write-behind action remove edge
    }

    @Override
    public Iterable<Edge> getEdges() {
        return FluentIterable.from(edgeCache.asMap().keySet())
                .append(mutatedEdges.keySet())
                .filter(not(in(deletedEdges)))
                .transform(CPIEdge.MAKE(this))
                .transform(CPIEdge.DOWNCAST);
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        return null;
    }

    // =======================================
    @Override
    @Deprecated
    public void stopTransaction(Conclusion conclusion) {

    }

    private void reset() {
        mutatedEdges.clear();
        mutatedVertices.clear();
        deletedVertices.clear();
        deletedEdges.clear();
        // TODO: ditch incremental indices?
    }

    @Override
    public void commit() {
        /*
        Merge order:
        1. remove edges
        2. remove vertices
        3. add vertices
        4. add edges
        5. remove properties (if element exists)
        6. set properties (if element exists)
        7. incremental indices? maybe auto-update on rm&set?
        queue write-behind
        */
        edgeCache.invalidateAll(deletedEdges);
        vertexCache.invalidateAll(deletedVertices);

        vertexCache.putAll(mutatedVertices);
        edgeCache.putAll(mutatedEdges);
        // TODO: incremental indices
        reset();
    }

    @Override
    public void rollback() {
        reset();
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

    // =======================================
    @Override
    public GraphQuery query() {
        return null;
    }

    @Override
    public void shutdown() {
        // TODO: how to coördinate?
    }

    // =======================================
    private final String graphId;
    private final CPIGraphFactory factory;
    final Cache<String, CPIVertex.Impl> vertexCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .build();
    final Cache<String, CPIEdge.Impl> edgeCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .build();

    Map<String, CPIVertex.Impl> mutatedVertices = newHashMap();
    Set<String> deletedVertices = newHashSet();
    Map<String, CPIEdge.Impl> mutatedEdges = newHashMap();
    Set<String> deletedEdges = newHashSet();
}
