package edu.utexas.arlut.ciads.cpiGraph;


import java.util.Set;
import java.util.concurrent.*;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.wrappers.WrapperGraph;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import static java.lang.Boolean.*;

@Slf4j
public class CPIGraphManager<T extends KeyIndexableGraph & WrapperGraph<T> & IndexableGraph & TransactionalGraph> {

    public CPIGraphManager(T systemOfRecord) {
        this.sor = systemOfRecord;
        vertexIdFactory = new IdFactory.DefaultIdFactory();
        edgeIdFactory = new IdFactory.DefaultIdFactory();
        executor = Executors.newSingleThreadExecutor();

        vIndex = resolvePartitionIndex(Vertex.class, V_PARTITION_IDX);
        eIndex = resolvePartitionIndex(Edge.class, E_PARTITION_IDX);

        // don't need a cached index, already kept by ID
//        sor.createKeyIndex(CPIGraph.ID, Vertex.class);
//        sor.createKeyIndex(CPIGraph.ID, Edge.class);

        // keep a cached index for each?
//        sor.getIndexedKeys(Vertex.class);
    }

    public CPIGraph create(String graphId) {
        CPIGraph g = new CPIGraph(graphId, this);
        load(g);
        return g;
    }

    public void shutdown() {
        for (CPIGraph g : graphs.asMap().values())
            g.shutdown();
        // TODO: timed wait?
        executor.shutdown();
        try {
            executor.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        sor.shutdown();
    }

    public CPIGraph getGraph(final String graphId) {
        try {
            return graphs.get(graphId, new Callable<CPIGraph>() {
                @Override
                public CPIGraph call() throws ExecutionException {
                    return new CPIGraph(graphId, CPIGraphManager.this);
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

    // =================================
    void addVertex(final CPIVertexProxy.CPIVertex v, final String partitionKey) {
        enqueue(new Runnable() {
            @Override
            public void run() {
                try {
                    log.info("SOR addVertex {}", v.getId());
                    Vertex sorV = sor.addVertex(v.getId());
                    vIndex.put(partitionKey, TRUE, sorV);
                    v.setBase(sorV);
                } catch (Exception e) {
                    log.error("SOR addVertex Exception", e);
                }
            }
        });
    }

    void removeVertex(final CPIVertexProxy.CPIVertex v) {
        enqueue(new Runnable() {
            @Override
            public void run() {
                try {
                    log.info("SOR removeVertex {}", v.getId());
                    Vertex sorV = v.getBase();
                    sor.removeVertex(sorV);
                } catch (Exception e) {
                    log.error("SOR removeVertex Exception", e);
                }
            }
        });
    }

    void setProperty(final CPIElementProxy.CPIElement e, final String key, final Object value) {
        enqueue(new Runnable() {
            @Override
            public void run() {
                try {
                    log.info("SOR setProperty {} {} => {}", e.getId(), key, value);
                    Element sorE = e.getBase();
                    if (null == sorE)
                        ; // TODO: need to populate if not exists!
                    sorE.setProperty(key, value);
                } catch (Exception e) {
                    log.error("SOR setProperty Exception", e);
                }
            }
        });
    }

    void removeProperty(final CPIVertexProxy.CPIVertex v, final String key) {
        enqueue(new Runnable() {
            @Override
            public void run() {
                try {
                    log.info("SOR removeProperty {} {}", v.getId(), key);
                    Vertex sorV = v.getBase();
                    if (null == sorV)
                        ; // TODO: need to populate if not exists!
                    sorV.removeProperty(key);
                } catch (Exception e) {
                    log.error("SOR removeProperty Exception", e);
                }
            }
        });
    }

    void commit() {
        enqueue(new Runnable() {
            @Override
            public void run() {
                sor.commit();
            }
        });
    }

    // =======================================
    String vertexId() {
        return vertexIdFactory.call();
    }

    String edgeId() {
        return edgeIdFactory.call();
    }

    // =======================================
    private <T extends Element> Index<T> resolvePartitionIndex(Class<T> clazz, String name) {
//        return sor.createIndex(name, clazz);
        Index<T> idx = sor.getIndex(name, clazz);
        return (null == idx) ? sor.createIndex(name, clazz)
                : idx;
    }

    private void load(CPIGraph g) {
        // load up from system of record
        for (Vertex v : vIndex.get(g.getId(), TRUE))
            g.fastAdd(v);
        for (Edge e : eIndex.get(g.getId(), TRUE))
            g.fastAdd(e);
    }

    void enqueue(Runnable r) {
        executor.submit(r);
    }

    // =================================
    static final String V_PARTITION_IDX = "partitionV";
    static final String E_PARTITION_IDX = "partitionE";
    private final Index<Vertex> vIndex;
    private final Index<Edge> eIndex;

    @Setter
    IdFactory vertexIdFactory;
    @Setter
    IdFactory edgeIdFactory;

    Cache<String, CPIGraph> graphs = CacheBuilder.newBuilder()
            .maximumSize(100)
            .expireAfterAccess(120, TimeUnit.MINUTES)
            .build();
    private final T sor;
    private final ExecutorService executor;
}