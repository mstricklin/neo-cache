package edu.utexas.arlut.ciads.cacheGraph;

import com.tinkerpop.blueprints.*;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

@Slf4j
public class GraphTransaction implements TransactionalGraph, KeyIndexableGraph {

    GraphTransaction(CachedGraph cg) {
        baselineGraph = cg;
    }
    @Override
    public Features getFeatures() {
        return baselineGraph.getFeatures();
    }
    void dump() {
        log.info("= Transaction {} =", Thread.currentThread().getId());
        for (Map.Entry<String, CachedVertex> me: addedVertices.entrySet())
            log.info("\t+ {} => {}", me.getKey(), me.getValue());
        log.info("\t- {}", deletedVertices);
        for (Map.Entry<String, CachedEdge> me: addedEdges.entrySet())
            log.info("\t+ {} => {}", me.getKey(), me.getValue());
        log.info("\t- {}", deletedEdges);
    }

    @Override
    public Vertex addVertex(Object id_) {
        String id = baselineGraph.vertexId();
        CachedVertex.Impl impl = new CachedVertex.Impl();
        mutatedVertices.put(id, impl);
        Action a = new Action.AddVertex(impl);
        return new CachedVertex(id, baselineGraph);
    }

    public CachedVertex.Impl getImpl(String id) {
        return null;
    }
    public CachedVertex.Impl getImplForMutation(String id) {
        return null;
    }

    @Override
    public Vertex getVertex(Object id) {
        if (deletedVertices.contains(id))
            return null;
        if (addedVertices.containsKey(id))
            return new CachedVertex(addedVertices.get(id), baselineGraph);
        return new CachedVertex(baselineGraph.getVertex(id), baselineGraph);
    }

    @Override
    public void removeVertex(Vertex v) {
        String id = v.getProperty(CachedGraph.ID);
        for (Edge e : v.getEdges(Direction.BOTH)) {
            removeEdge(e);
        }
//        Action a = new Action.RemoveVertex(impl);
        addedVertices.remove(id);
        deletedVertices.add(id);
    }

    @Override
    public Iterable<Vertex> getVertices() {
        return null;
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        return null;
    }
    // =======================================
    @Override
    public Edge addEdge(Object id_, Vertex outVertex, Vertex inVertex, String label) {
        String id = baselineGraph.edgeId();
        CachedVertex oV = (CachedVertex) outVertex;
        CachedVertex iV = (CachedVertex) inVertex;

        final CachedEdge ce = new CachedEdge(id, oV, iV, label, baselineGraph);
        oV.addOutEdge(ce); // TODO: these need to be transactional vertices
        iV.addInEdge(ce);

        addedEdges.put(id, ce);
        return ce;
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
    // =======================================
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

    private final CachedGraph baselineGraph;

    final Map<String, CachedVertex.Impl> mutatedVertices = newHashMap();
    final Set<String> deletedVertices = newHashSet();

    final Map<String, CachedVertex> addedVertices = newHashMap();
    final Set<String> _deletedVertices = newHashSet();

    final Map<String, CachedEdge> addedEdges = newHashMap();
    final Set<String> deletedEdges = newHashSet();


}
