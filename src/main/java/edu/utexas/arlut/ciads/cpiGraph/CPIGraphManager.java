package edu.utexas.arlut.ciads.cpiGraph;


import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.*;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.wrappers.WrapperGraph;
import edu.utexas.arlut.ciads.cpiGraph.CPIEdgeProxy.CPIEdge;
import edu.utexas.arlut.ciads.cpiGraph.CPIElementProxy.CPIElement;
import edu.utexas.arlut.ciads.cpiGraph.CPIVertexProxy.CPIVertex;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.getOnlyElement;
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

        // tweak element lookups for write-behinds
        sor.createKeyIndex(CPIGraph.ID, Vertex.class);
        sor.createKeyIndex(CPIGraph.ID, Edge.class);
        sor.createKeyIndex(CPIGraph.PARTITION, Vertex.class);
        sor.createKeyIndex(CPIGraph.PARTITION, Edge.class);
    }

    public CPIGraph create(String graphId) {
        CPIGraph g = new CPIGraph(graphId, this);
        load(g);
        return g;
    }
    public CPIGraph createFrom(CPIGraph src, String graphId) {
        CPIGraph g = new CPIGraph(src, graphId, this);
//        load(g);

        return g;
    }

    public void shutdown() {
        for (CPIGraph g : graphs.asMap().values())
            g.shutdown();
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
    Runnable addVertex(final String partition, final String id) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    log.info("SOR addVertex {}", id);
                    Vertex sorV = sor.addVertex(id);
                    vIndex.put(partition, TRUE, sorV);
                    sorV.setProperty(CPIGraph.PARTITION, partition);
                    sorV.setProperty(CPIGraph.ID, id);
                } catch (Exception e) {
                    log.error("SOR addVertex Exception", e);
                }
            }
        };
    }

    Runnable removeVertex(final String partition, final String id) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    Vertex sorV = lookupVertex(partition, id);
                    log.info("SOR removeVertex {} {}", id, sorV);
                    sor.removeVertex(sorV);
                } catch (Exception e) {
                    log.error("SOR removeVertex Exception", e);
                }
            }
        };
    }

    Runnable addEdge(final String partition, final String id, final String ovID, final String ivID, final String label) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    log.info("SOR addEdge {}", id);
                    Vertex oSorV = lookupVertex(partition, ovID);
                    Vertex iSorV = lookupVertex(partition, ivID);
                    Edge sorE = sor.addEdge(id, oSorV, iSorV, label);
                    eIndex.put(partition, TRUE, sorE);

                    sorE.setProperty(CPIGraph.PARTITION, partition);
                    sorE.setProperty(CPIGraph.ID, id);
                } catch (Exception e) {
                    log.error("SOR addEdge Exception", e);
                }
            }
        };
    }

    Runnable removeEdge(final String partition, final String id) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    Edge sorE = lookupEdge(partition, id);
                    log.info("SOR removeEdge {} {}", id, sorE);
                    sor.removeEdge(sorE);
                } catch (Exception e) {
                    log.error("SOR removeEdge Exception", e);
                }
            }
        };
    }

    Runnable setVProperty(final String partition, final String id, final String key, final Object value) {
        return new Runnable() {
            @Override
            public void run() {
                Element sorE = lookupVertex(partition, id);
                setProperty(sorE, key, value);
            }
        };
    }
    Runnable setEProperty(final String partition, final String id, final String key, final Object value) {
        return new Runnable() {
            @Override
            public void run() {
                Element sorE = lookupEdge(partition, id);
                setProperty(sorE, key, value);
            }
        };
    }

    private void setProperty(final Element sorE, final String key, final Object value) {
        try {
            log.info("SOR setProperty {} {} => {}", sorE, key, value);
            sorE.setProperty(key, value);
        } catch (Exception e) {
            log.error("SOR setProperty Exception", e);
        }
    }

    Runnable removeVProperty(final String partition, final String id, final String key) {
        return new Runnable() {
            @Override
            public void run() {
                Element sorE = lookupVertex(partition, id);
                removeProperty(sorE, key);
            }
        };
    }
    Runnable removeEProperty(final String partition, final String id, final String key) {
        return new Runnable() {
            @Override
            public void run() {
                Element sorE = lookupEdge(partition, id);
                removeProperty(sorE, key);
            }
        };
    }
    private void removeProperty(final Element sorE, final String key) {
        try {
            log.info("SOR removeProperty {} {}", sorE, key);
            sorE.removeProperty(key);
        } catch (Exception e) {
            log.error("SOR removeProperty Exception", e);
        }
    }

    Runnable commit() {
        return new Runnable() {
            @Override
            public void run() {
                log.info("SOR commit");
                sor.commit();
            }
        };
    }

    Runnable rollback() {
        return new Runnable() {
            @Override
            public void run() {
                log.info("SOR rollback");
                sor.rollback();
            }
        };
    }

    private Vertex lookupVertex(String partition, String id) {
        try {
            return getOnlyElement(sor.query().has(CPIGraph.ID, id)
                    .has(CPIGraph.PARTITION, partition)
                    .vertices());
        } catch (NoSuchElementException e) {
            log.error("No vertex found with id {}", id);
            throw e;
        } catch (IllegalArgumentException e) {
            log.error("Multiple vertices found with id {}", id);
            return getFirst(sor.getVertices(CPIGraph.ID, id), null);
        }
    }

    private Edge lookupEdge(String partition, String id) {
        try {
            return getOnlyElement(sor.query().has(CPIGraph.ID, id)
                    .has(CPIGraph.PARTITION, partition)
                    .edges());
        } catch (NoSuchElementException e) {
            log.error("No vertex found with id {}", id);
            throw e;
        } catch (IllegalArgumentException e) {
            log.error("Multiple edges found with id {}", id);
            return getFirst(sor.query().has(CPIGraph.ID, id).has(partition, true).edges(), null);
        }
    }

    void queue(Queue<Runnable> q) {
        // lock...
        while ( ! q.isEmpty() ) {
            executor.submit( q.remove() );
        }
    }

    // =======================================
    String vertexId(String id) {
        if (null != id)
            return id;
        return vertexIdFactory.call();
    }

    String edgeId(String id) {
        if (null != id)
            return id;
        return edgeIdFactory.call();
    }

    // =======================================
    private <E extends Element> Index<E> resolvePartitionIndex(Class<E> clazz, String name) {
//        return sor.createIndex(name, clazz);
        Index<E> idx = sor.getIndex(name, clazz);
        return (null == idx) ? sor.createIndex(name, clazz)
                : idx;
    }

    private void load(CPIGraph g) {
        // load up from system of record
        for (Vertex v : vIndex.get(g.getId(), TRUE))
            g.rawAdd(v);
        for (Edge e : eIndex.get(g.getId(), TRUE))
            g.rawAdd(e);
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

    private Cache<String, CPIGraph> graphs = CacheBuilder.newBuilder()
            .maximumSize(100)
            .expireAfterAccess(120, TimeUnit.MINUTES)
            .build();
    private final T sor;
    private final ExecutorService executor;
}
