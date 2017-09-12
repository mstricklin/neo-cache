package edu.utexas.arlut.ciads.cpiGraph;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Queues.newArrayDeque;
import static com.google.common.collect.Sets.newHashSet;
import static com.tinkerpop.blueprints.util.ExceptionFactory.edgeLabelCanNotBeNull;

import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.*;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.PropertyFilteredIterable;
import com.tinkerpop.blueprints.util.StringFactory;
import edu.utexas.arlut.ciads.cpiGraph.CPIEdgeProxy.CPIEdge;
import edu.utexas.arlut.ciads.cpiGraph.CPIVertexProxy.CPIVertex;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CPIGraph implements KeyIndexableGraph, TransactionalGraph {

    public static final String ID = "__id";
    public static final String PARTITION = "__partition";

    CPIGraph(String graphId, CPIGraphManager manager) {
        log.info("Init CPIGraph w/ id '{}'", graphId);
        this.graphId = graphId;
        this.manager = manager;
    }
    CPIGraph(CPIGraph src, String graphId, CPIGraphManager manager) {
        // TODO: queue up W-B & commit
        log.info("Init CPIGraph from {} w/ id '{}'", src.graphId, graphId);
        this.graphId = graphId;
        this.manager = manager;
        for (Map.Entry<String, CPIVertex> ve: src.vertexCache.asMap().entrySet()) {
            CPIVertex impl = new CPIVertex(ve.getValue());
            vertexCache.put(ve.getKey(), impl);
            manager.addVertex(graphId, impl.getId());
            for (Map.Entry<String, Object> pe: impl.properties.entrySet()) {
                manager.setVProperty(graphId, impl.getId(), pe.getKey(), pe.getValue());
            }
        }
        for (Map.Entry<String, CPIEdge> ve: src.edgeCache.asMap().entrySet()) {
            CPIEdge impl = new CPIEdge(ve.getValue());
            edgeCache.put(ve.getKey(), impl);
            manager.addEdge(graphId, impl.getId(), impl.outVertexId, impl.inVertexId, impl.label);
            for (Map.Entry<String, Object> pe: impl.properties.entrySet())
                manager.setEProperty(graphId, impl.getId(), pe.getKey(), pe.getValue());
        }
    }

    public String getId() {
        return graphId;
    }

    private static final Features FEATURES = new Features();

    static {
        // TODO: revisit this...
        FEATURES.supportsSerializableObjectProperty = true; // TODO will this survive serialization?
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = true;
        FEATURES.supportsUniformListProperty = true;
        FEATURES.supportsMixedListProperty = true; // TODO will this survive serialization?
        FEATURES.supportsLongProperty = true;
        FEATURES.supportsMapProperty = true; // TODO will this survive serialization?
        FEATURES.supportsStringProperty = true;

        FEATURES.supportsDuplicateEdges = true;
        FEATURES.supportsSelfLoops = true;
        FEATURES.isPersistent = true;
        FEATURES.isWrapper = false;
        FEATURES.supportsVertexIteration = true;
        FEATURES.supportsEdgeIteration = true;
        FEATURES.supportsVertexIndex = true;
        FEATURES.supportsEdgeIndex = true;
        FEATURES.ignoresSuppliedIds = true;
        FEATURES.supportsTransactions = true;
        FEATURES.supportsIndices = true;
        FEATURES.supportsKeyIndices = true;
        FEATURES.supportsVertexKeyIndex = true;
        FEATURES.supportsEdgeKeyIndex = true;
        FEATURES.supportsEdgeRetrieval = true;
        FEATURES.supportsVertexProperties = true;
        FEATURES.supportsEdgeProperties = true;
        FEATURES.supportsThreadedTransactions = false;
    }


    @Override
    public Features getFeatures() {
        return FEATURES;
    }

    // =================================
    public static IllegalArgumentException deletedElementException(String id) {
        return new IllegalArgumentException("Element " + id + " has been deleted");
    }

    // =================================
    CPIVertex vertexImpl(String id) {
        if (deletedVertices.contains(id))
            throw deletedElementException(id);
        return mutatedVertices.containsKey(id) ? mutatedVertices.get(id)
                : vertexCache.getIfPresent(id);
        // TODO: exception on not present...
    }

    CPIVertex mutableVertexImpl(String id) {
        if (deletedVertices.contains(id))
            throw deletedElementException(id);
        if (mutatedVertices.containsKey(id)) {
            return mutatedVertices.get(id);
        }
        CPIVertex v = new CPIVertex(vertexCache.getIfPresent(id));
        mutatedVertices.put(id, v);
        return v;
        // TODO: exception on not present...
    }

    CPIEdge edgeImpl(String id) {
        if (deletedEdges.contains(id))
            throw deletedElementException(id);
        return mutatedEdges.containsKey(id) ? mutatedEdges.get(id)
                : edgeCache.getIfPresent(id);
        // TODO: exception on not present...
    }

    CPIEdge mutableEdgeImpl(String id) {
        if (deletedEdges.contains(id))
            throw deletedElementException(id);
        if (mutatedEdges.containsKey(id)) {
            return mutatedEdges.get(id);
        }
        CPIEdge e = new CPIEdge(edgeCache.getIfPresent(id));
        mutatedEdges.put(id, e);
        return e;
        // TODO: exception on not present...
    }

    // =================================
    void rawAdd(Vertex v) {
        String id = firstNonNull(v.getProperty(ID), v.getId()).toString();
        CPIVertex impl = new CPIVertex(id);
        impl.putProperties(v);
        vertexCache.put(id, impl);
    }

    void rawAdd(Edge e) {
        String id = firstNonNull(e.getProperty(ID), e.getId()).toString();

        Vertex outVertex = e.getVertex(Direction.OUT);
        String outID = firstNonNull(outVertex.getProperty(ID), outVertex.getId()).toString();
        vertexImpl(outID).outEdges.add(id);

        Vertex inVertex = e.getVertex(Direction.OUT);
        String inID = firstNonNull(inVertex.getProperty(ID), inVertex.getId()).toString();
        vertexImpl(inID).inEdges.add(id);

        CPIEdge impl = new CPIEdge(id, outID, inID, e.getLabel());
        impl.putProperties(e);
        edgeCache.put(id, impl);
    }

    // =================================
    @Override
    public Vertex addVertex(Object id_) {
        String id = manager.vertexId(id_.toString());
        CPIVertex impl = new CPIVertex(id);
        mutatedVertices.put(id, impl);
        impl.properties.put(ID, id);

        queueAction(manager.addVertex(graphId, id));

        return new CPIVertexProxy(id, this);
    }

    @Override
    public Vertex getVertex(Object id) {
        if (null == id)
            throw com.tinkerpop.blueprints.util.ExceptionFactory.vertexIdCanNotBeNull();
        if (deletedVertices.contains(id))
            return null;
        if (mutatedVertices.containsKey(id.toString())
                || (null != vertexCache.getIfPresent(id.toString())))
            return new CPIVertexProxy(id.toString(), this);
        return null;
    }

    @Override
    public void removeVertex(Vertex vertex) {
        checkNotNull(vertex);
        for (Edge e : vertex.getEdges(Direction.BOTH))
            removeEdge(e);

        String id = vertex.getId().toString();
        mutatedVertices.remove(id);
        deletedVertices.add(id);

        queueAction(manager.removeVertex(graphId, id));
    }


    @Override
    public Iterable<Vertex> getVertices() {
        return FluentIterable.from(vertexCache.asMap().keySet())
                .append(mutatedVertices.keySet())
                .filter(not(in(deletedVertices)))
                .transform(CPIVertexProxy.MAKE(this))
                .transform(CPIVertexProxy.DOWNCAST)
                .toList();
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        if (vIndices.has(key)) {
            return FluentIterable.from(vIndices.indexed(key, value))
                    .transform(CPIVertexProxy.PROXY(this))
                    .transform(CPIVertexProxy.DOWNCAST)
                    .toList();
        }
        return new PropertyFilteredIterable<>(key, value, this.getVertices());
    }

    @Override
    public Edge addEdge(Object id_, Vertex outVertex, Vertex inVertex, String label) {
        // TODO: assert non-null & cast-ability
        if (label == null)
            throw edgeLabelCanNotBeNull();
        String id = manager.edgeId(id_.toString());
        CPIEdge impl = new CPIEdge(id,
                outVertex.getId().toString(),
                inVertex.getId().toString(),
                label);
        mutatedEdges.put(id, impl);
        impl.properties.put(ID, id);

        CPIVertexProxy oVP = (CPIVertexProxy) outVertex;
        oVP.addOutEdge(id);
        CPIVertexProxy iVP = (CPIVertexProxy) inVertex;
        iVP.addInEdge(id);

        CPIVertex oVImpl = mutableVertexImpl(outVertex.getId().toString());
        CPIVertex iVImpl = mutableVertexImpl(inVertex.getId().toString());

        queueAction(manager.addEdge(graphId, id, oVP.rawId(), iVP.rawId(), label));

        return new CPIEdgeProxy(id, this);
    }

    @Override
    public Edge getEdge(Object id) {
        if (null == id)
            throw com.tinkerpop.blueprints.util.ExceptionFactory.edgeIdCanNotBeNull();
        if (deletedEdges.contains(id))
            return null;
        if (mutatedEdges.containsKey(id.toString())
                || (null != edgeCache.getIfPresent(id.toString())))
            return new CPIEdgeProxy(id.toString(), this);
        return null;
    }

    @Override
    public void removeEdge(Edge edge) {
        checkNotNull(edge);

        CPIEdgeProxy ep = (CPIEdgeProxy) edge;
        String id = ep.rawId();

        CPIVertexProxy vOut = ep.outVertex();
        vOut.removeEdge(id);

        CPIVertexProxy vIn = ep.inVertex();
        vIn.removeEdge(id);

        mutatedEdges.remove(id);
        deletedEdges.add(id);

        queueAction(manager.removeEdge(graphId, id));
    }

    @Override
    public Iterable<Edge> getEdges() {
        return FluentIterable.from(edgeCache.asMap().keySet())
                .append(mutatedEdges.keySet())
                .filter(not(in(deletedEdges)))
                .transform(CPIEdgeProxy.MAKE(this))
                .transform(CPIEdgeProxy.DOWNCAST)
                .toList();
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        if (eIndices.has(key)) {
            return FluentIterable.from(eIndices.indexed(key, value))
                    .transform(CPIEdgeProxy.PROXY(this))
                    .transform(CPIEdgeProxy.DOWNCAST)
                    .toList();
        }
        return new PropertyFilteredIterable<>(key, value, this.getEdges());
    }

    // =================================
    @Override
    public String toString() {
        return StringFactory.graphString(this, graphId);
    }

    @Override
    public int hashCode() {
        return graphId.hashCode();
    }

    // =================================
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

        queueAction(manager.commit());
        // now fire them off
        manager.queue(wbActions);

        // TODO: incremental indices
        reset();
    }

    @Override
    public void rollback() {
        reset();
    }

    // =======================================
    CPIIndex<CPIVertex> vIndices = new CPIIndex<>();
    CPIIndex<CPIEdge> eIndices = new CPIIndex<>();

    @Override
    public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, Parameter... indexParameters) {
        checkNotNull(elementClass);

        if (Vertex.class.isAssignableFrom(elementClass)) {
            vIndices.addIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            eIndices.addIndex(key);
        } else {
            throw ExceptionFactory.classIsNotIndexable(elementClass);
        }
        // TODO: write to SOR
    }

    @Override
    public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
        checkNotNull(elementClass);

        if (Vertex.class.isAssignableFrom(elementClass)) {
            vIndices.dropIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            eIndices.dropIndex(key);
        } else {
            throw ExceptionFactory.classIsNotIndexable(elementClass);
        }
        // TODO: write to SOR
    }

    @Override
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return vIndices.keys();
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            return eIndices.keys();
        } else {
            throw ExceptionFactory.classIsNotIndexable(elementClass);
        }
    }

    // =======================================
    @Override
    public GraphQuery query() {
        return new DefaultGraphQuery(this);
    }

    @Override
    public void shutdown() {
        // TODO: how to coördinate?
        // 1. flush all write-behind
        // 2. tell manager to remove us
//        manager.commit();
    }

    void queueAction(Runnable r) {
        wbActions.add(r);
    }
    // =======================================
    private final String graphId;
    final CPIGraphManager manager;
    final Cache<String, CPIVertex> vertexCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .build();
    final Cache<String, CPIEdge> edgeCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .build();

    Map<String, CPIVertex> mutatedVertices = newHashMap();
    Set<String> deletedVertices = newHashSet();
    Map<String, CPIEdge> mutatedEdges = newHashMap();
    Set<String> deletedEdges = newHashSet();

    Queue<Runnable> wbActions = newArrayDeque();
}
