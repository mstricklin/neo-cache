package edu.utexas.arlut.ciads.cpiGraph;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import lombok.extern.slf4j.Slf4j;

import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Boolean.TRUE;

@Slf4j
public class CPIWriteBehind {

    CPIWriteBehind(TransactionalGraph sor) {
        this.sor = sor;
    }


    void await() {
        try {
            log.info("start await");
            executor.awaitTermination(10, TimeUnit.SECONDS);
            log.info("end await");

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void stats() {
        log.info("vCache stats {}", vertexCache.stats());
        log.info("eCache stats {}", edgeCache.stats());
    }


    // =======================================
    void addVertex(final String id) {
        executor.submit(() -> {
            try {
                Vertex sorV = sor.addVertex(id);
                log.info("SOR addVertex {} => {}", id, sorV);
                vertexCache.put(id, sorV);
                sorV.setProperty(CPIGraph.ID, id);
            } catch (Exception e) {
                log.error("SOR addVertex Exception", e);
            }
        });
    }

    void removeVertex(final String id) {
        executor.submit(() -> {
            try {
                Vertex sorV = lookupVertex(id);
                log.info("SOR removeVertex {} {}", id, sorV);
                sor.removeVertex(sorV);
            } catch (Exception e) {
                log.error("SOR removeVertex Exception", e);
            }
        });
    }

    void addEdge(final String id, final String ovID, final String ivID, final String label) {
        executor.submit(() -> {
            try {
                Vertex oSorV = lookupVertex(ovID);
                Vertex iSorV = lookupVertex(ivID);
                Edge sorE = sor.addEdge(id, oSorV, iSorV, label);
                log.info("SOR addEdge {} {}", id, sorE);
                sorE.setProperty(CPIGraph.ID, id);
            } catch (Exception e) {
                log.error("SOR addEdge Exception", e);
            }
        });
    }

    void removeEdge(final String id) {
        executor.submit(() -> {
            try {
                Edge sorE = lookupEdge(id);
                log.info("SOR removeEdge {} {}", id, sorE);
                sor.removeEdge(sorE);
            } catch (Exception e) {
                log.error("SOR removeEdge Exception", e);
            }
        });
    }

    void setVProperty(final String id, final String key, final Object value) {
        executor.submit(() -> setProperty(lookupVertex(id), key, value));
    }

    void setEProperty(final String id, final String key, final Object value) {
        executor.submit(() -> setProperty(lookupEdge(id), key, value));
    }

    private void setProperty(final Element sorE, final String key, final Object value) {
        try {
            log.info("SOR setProperty {} {} => {}", sorE, key, value);
            sorE.setProperty(key, value);
        } catch (Exception e) {
            log.error("SOR setProperty Exception", e);
        }
    }

    void removeVProperty(final String id, final String key) {
        executor.submit(() -> removeProperty(lookupVertex(id), key));
    }

    void removeEProperty(final String id, final String key) {
        executor.submit(() -> removeProperty(lookupEdge(id), key));
    }

    private void removeProperty(final Element sorE, final String key) {
        try {
            log.info("SOR removeProperty {} {}", sorE, key);
            sorE.removeProperty(key);
        } catch (Exception e) {
            log.error("SOR removeProperty Exception", e);
        }
    }

    void commit() {
        // TODO: invalidate cache
        executor.submit(() -> {
                    log.info("SOR commit");
                    sor.commit();
                    vertexCache.invalidateAll();
                    log.info("vCache stats {}", vertexCache.stats());
                    log.info("eCache stats {}", edgeCache.stats());
                }
        );
    }

    void rollback() {
        executor.submit(() -> {
            log.info("SOR rollback");
            sor.rollback();
            vertexCache.invalidateAll();
        });
    }

    void shutdown() {
        executor.submit(() -> {
            executor.shutdown();
            log.info("sor shutdown");
            sor.shutdown();
            vertexCache.invalidateAll();
        });
    }

    // =======================================
    private Vertex lookupVertex(String id) {
        try {
            return vertexCache.get(id);
        } catch (Exception e) {
            log.error("Error looking up vertex with id {}", id, e);
            throw new NoSuchElementException();
        }
    }

    private Edge lookupEdge(String id) {
        try {
            return edgeCache.get(id);
        } catch (ExecutionException e) {
            log.error("Error looking up vertex with id {}", id);
            throw new NoSuchElementException();
        }
    }
    // =======================================
    final LoadingCache<String, Vertex> vertexCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .recordStats()
            .build(new CacheLoader<String, Vertex>() {
                       public Vertex load(String id) {
                           log.info("vertexCache load {}", id);
                           return getOnlyElement(sor.getVertices(CPIGraph.ID, id));
                       }
                   }
            );
    final LoadingCache<String, Edge> edgeCache = CacheBuilder.newBuilder()
            .maximumSize(2000)
            .build(new CacheLoader<String, Edge>() {
                       public Edge load(String id) {
                           log.info("edgeCache load {}", id);
                           return getOnlyElement(sor.getEdges(CPIGraph.ID, id));
                       }
                   }
            );
    // =======================================
    private final TransactionalGraph sor;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
}
