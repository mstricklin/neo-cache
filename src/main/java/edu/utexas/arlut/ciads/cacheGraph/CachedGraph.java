package edu.utexas.arlut.ciads.cacheGraph;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.*;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.PropertyFilteredIterable;
import com.tinkerpop.blueprints.util.StringFactory;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;

// CPI =
// Cached
// Partitioned
// IDed
@Slf4j
public class CachedGraph<T extends TransactionalGraph & KeyIndexableGraph>
        implements TransactionalGraph, KeyIndexableGraph {
    public static final String ID = "__id";
    private final T graph;
    final Cache<String, CachedVertex> vertexCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .build();
    final Cache<String, CachedEdge> edgeCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .build();

    @Setter
    IdFactory vertexIdFactory;
    @Setter
    IdFactory edgeIdFactory;

    public class NullWriteBehind extends WriteBehind {
    }

    WriteBehind writeBehind = new NullWriteBehind();


    public CachedGraph(T graph) {
        this.graph = graph;
        vertexIdFactory = new IdFactory.DefaultIdFactory();
        edgeIdFactory = new IdFactory.DefaultIdFactory();

        createKeyIndex(ID, Vertex.class);
        createKeyIndex(ID, Edge.class);

        // load with NullWriteBehind, so we don't write to a graph we're loading from
        initialLoad();
        // then switch over to a real backing graph
        writeBehind = new CacheWB<T>(graph);
    }

    private void initialLoad() {
        for (String ik : graph.getIndexedKeys(Vertex.class))
            createKeyIndex(ik, Vertex.class);
        for (String ik : graph.getIndexedKeys(Edge.class))
            createKeyIndex(ik, Edge.class);
        for (Vertex v : graph.getVertices())
            _addVertex(v);
        for (Edge e : graph.getEdges())
            _addEdge(e);
    }

    public void dump() {
        log.info("= baseline =");
        for (Map.Entry<String, CachedVertex> me : vertexCache.asMap().entrySet())
            log.info("\t{} => {}", me.getKey(), me.getValue());
        for (Map.Entry<String, CachedEdge> me : edgeCache.asMap().entrySet())
            log.info("\t{} => {}", me.getKey(), me.getValue());
        getTx().dump();
    }

    @Override
    public Features getFeatures() {
        return graph.getFeatures();
    }

    @Override
    public Vertex addVertex(Object id_) {
        return getTx().addVertex(null);
    }

    private void _addVertex(Vertex v) {
        final CachedVertex cv = new CachedVertex(v, this);
        String id = cv.getId().toString();
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
        for (Edge e : v.getEdges(Direction.BOTH)) {
            removeEdge(e);
        }
        vertexCache.invalidate(id);
        writeBehind.removeVertex((CachedVertex) v);
    }

    @Override
    public Iterable<Vertex> getVertices() {
//        return ImmutableList.copyOf(vertexCache.asMap().values());
        return Collections.emptyList();
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
        CachedVertex oV = (CachedVertex) outVertex;
        CachedVertex iV = (CachedVertex) inVertex;

        final CachedEdge ce = new CachedEdge(id, oV, iV, label, this);
        oV.addOutEdge(ce);
        iV.addInEdge(ce);

        edgeCache.put(id, ce);
        writeBehind.addEdge(ce);
        return ce;
    }

    private CachedVertex resolve(Edge e, Direction d) {
        Vertex ov = e.getVertex(d);
        String ovid = ov.getProperty(ID).toString();
        return vertexCache.getIfPresent(ovid);
    }

    private void _addEdge(Edge e) {
        CachedVertex oV = resolve(e, Direction.OUT);
        CachedVertex iV = resolve(e, Direction.IN);
        final CachedEdge ce = new CachedEdge(e, oV, iV, this);
        String id = ce.getId().toString();
        edgeCache.put(id, ce);
    }

    @Override
    public Edge getEdge(Object id) {
        return edgeCache.getIfPresent(id.toString());
    }

    @Override
    public void removeEdge(Edge e) {
        String id = e.getProperty(ID);
        edgeCache.invalidate(id);
        writeBehind.removeEdge((CachedEdge) e);
    }

    @Override
    public Iterable<Edge> getEdges() {
//        return ImmutableList.copyOf(edgeCache.asMap().values());
        return Collections.emptyList();
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

    @Deprecated
    @Override
    public void stopTransaction(Conclusion conclusion) {

    }

    @Override
    public void shutdown() {
        log.info("CachedGraph shutdown");
        writeBehind.shutdown();
        graph.shutdown();
    }

    @Override
    public void commit() {
    /*
    o remove edges
    o remove vertices
    o add vertices
    o add edges
     */
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
    <T extends Element> boolean isIndexed(String key, Class<T> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass))
            return vertexIndex.containsKey(key);
        else if (Edge.class.isAssignableFrom(elementClass))
            return edgeIndex.containsKey(key);
        return false;
    }

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
            if (!isIndexed(key, elementClass)) {
                Multimap<Object, CachedVertex> mm = LinkedHashMultimap.create();
                mm = Multimaps.synchronizedMultimap(mm);
                vertexIndex.put(key, mm);
                writeBehind.addKeyIndex(key, elementClass);
            }
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            if (!isIndexed(key, elementClass)) {
                Multimap<Object, CachedEdge> mm = LinkedHashMultimap.create();
                mm = Multimaps.synchronizedMultimap(mm);
                edgeIndex.put(key, mm);
                writeBehind.addKeyIndex(key, elementClass);
            }
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
    void addToIndex(String key, Object value, CachedVertex vertex) {
        if (vertexIndex.containsKey(key)) {
            log.info("Add to index {}:{}:{}", key, value, vertex);
            vertexIndex.get(key).put(value, vertex);
        }
    }

    void removeFromIndex(String key, Object value, CachedVertex edge) {
        if (vertexIndex.containsKey(key))
            vertexIndex.get(key).remove(value, edge);
    }

    void addToIndex(String key, Object value, CachedEdge edge) {
        if (edgeIndex.containsKey(key)) {
            log.info("Add to index {}:{}:{}", key, value, edge);
            edgeIndex.get(key).put(value, edge);
        }
    }

    void removeFromIndex(String key, Object value, CachedEdge edge) {
        if (edgeIndex.containsKey(key))
            edgeIndex.get(key).remove(value, edge);
    }

    <T extends Element> void addToIndex(String key, Object value, T e) {
        if ((e instanceof CachedVertex) && (vertexIndex.containsKey(key))) {
            vertexIndex.get(key).put(value, (CachedVertex) e);
        } else if ((e instanceof CachedEdge) && (edgeIndex.containsKey(key))) {
            edgeIndex.get(key).put(value, (CachedEdge) e);
        }
    }

    // =======================================
    String vertexId() {
        return vertexIdFactory.call();
    }

    String edgeId() {
        return edgeIdFactory.call();
    }

    // =======================================
    // [vertex|edge]:key:value:Set<Element>
    Map<String, Multimap<Object, CachedVertex>> vertexIndex = newHashMap();
    Map<String, Multimap<Object, CachedEdge>> edgeIndex = newHashMap();

    private GraphTransaction getTx() {
        return tlGraphTransaction.get();
    }

    private ThreadLocal<GraphTransaction> tlGraphTransaction = new ThreadLocal<GraphTransaction>() {
        @Override
        protected GraphTransaction initialValue() {
            return new GraphTransaction(CachedGraph.this);
        }
    };
}
