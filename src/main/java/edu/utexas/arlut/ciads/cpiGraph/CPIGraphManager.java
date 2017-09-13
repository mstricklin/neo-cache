package edu.utexas.arlut.ciads.cpiGraph;


import java.util.concurrent.*;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.tinkerpop.blueprints.*;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import static com.google.common.collect.Iterables.getOnlyElement;

@Slf4j
public class CPIGraphManager<T extends KeyIndexableGraph & TransactionalGraph> {

    private final SoRBuilder<T> sorBuilder;
    public CPIGraphManager(SoRBuilder<T> sorBuilder) {
        this.sorBuilder = sorBuilder;
        vertexIdFactory = new IdFactory.DefaultIdFactory();
        edgeIdFactory = new IdFactory.DefaultIdFactory();

    }

    public CPIGraph create(String graphId) {
        TransactionalGraph tg = buildSOR(graphId);
        // TODO: load
        CPIWriteBehind wb = new CPIWriteBehind(tg);
        CPIGraph g = new CPIGraph(graphId, wb);
        load(g);
        return g;
    }
    public CPIGraph createFrom(CPIGraph src, String graphId) {
        TransactionalGraph tg = buildSOR(graphId);
        // TODO: load?
        CPIWriteBehind wb = new CPIWriteBehind(tg);
        CPIGraph g = new CPIGraph(src, graphId, wb);
        return g;
    }

    private T buildSOR(String graphId) {
        T t = sorBuilder.build(graphId);
        t.createKeyIndex(CPIGraph.ID, Vertex.class);
        t.createKeyIndex(CPIGraph.ID, Edge.class);
        return t;
    }

    public void shutdown() {
        for (CPIGraph g : graphs.asMap().values())
            g.shutdown();
    }

    public CPIGraph getGraph(final String graphId) {
        try {
            return graphs.get(graphId, new Callable<CPIGraph>() {
                @Override
                public CPIGraph call() throws ExecutionException {
                    return create(graphId);
                }
            });
        } catch (ExecutionException e) {
            log.error("error trying to create CPIGraph for {}", graphId, e);
        }
        return null;
    }

    public boolean graphExists(String graphId) {
        return (null != graphs.getIfPresent(graphId));
    }

    // =======================================
    static String vertexId(Object id) {
        if (null != id)
            return id.toString();
        return vertexIdFactory.call();
    }

    static String edgeId(Object id) {
        if (null != id)
            return id.toString();
        return edgeIdFactory.call();
    }

    // =======================================
//    private <E extends Element> Index<E> resolvePartitionIndex(Class<E> clazz, String name) {
//        return sor.createIndex(name, clazz);
//        Index<E> idx = sor.getIndex(name, clazz);
//        return (null == idx) ? sor.createIndex(name, clazz)
//                : idx;
//    }

    private void load(CPIGraph g) {
        // load up from system of record
//        for (Vertex v : vIndex.buildSOR(g.getId(), TRUE))
//            g.rawAdd(v);
//        for (Edge e : eIndex.buildSOR(g.getId(), TRUE))
//            g.rawAdd(e);
    }

    // =================================
//    static final String V_PARTITION_IDX = "partitionV";
//    static final String E_PARTITION_IDX = "partitionE";
//    private final Index<Vertex> vIndex;
//    private final Index<Edge> eIndex;

    @Setter
    static IdFactory vertexIdFactory;
    @Setter
    static IdFactory edgeIdFactory;

    private Cache<String, CPIGraph> graphs = CacheBuilder.newBuilder()
            .maximumSize(100)
            .expireAfterAccess(120, TimeUnit.MINUTES)
            .build();
}
