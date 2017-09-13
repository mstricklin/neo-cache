package edu.utexas.arlut.ciads.cpiGraph;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import lombok.extern.slf4j.Slf4j;

import java.util.NoSuchElementException;
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

    void shutdown() {
        flush();
        sor.shutdown();
    }

    static void flush() {
        executor.shutdown();
        try {
            executor.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // =======================================
    void addVertex(final String id) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    log.info("SOR addVertex {}", id);
                    Vertex sorV = sor.addVertex(id);
                    sorV.setProperty(CPIGraph.ID, id);
                } catch (Exception e) {
                    log.error("SOR addVertex Exception", e);
                }
            }
        });
    }

    void removeVertex(final String id) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Vertex sorV = lookupVertex(id);
                    log.info("SOR removeVertex {} {}", id, sorV);
                    sor.removeVertex(sorV);
                } catch (Exception e) {
                    log.error("SOR removeVertex Exception", e);
                }
            }
        });
    }

    void addEdge(final String id, final String ovID, final String ivID, final String label) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Vertex oSorV = lookupVertex(ovID);
                    Vertex iSorV = lookupVertex(ivID);
                    Edge sorE = sor.addEdge(id, oSorV, iSorV, label);
                    log.info("SOR addEdge {} {}", id, sorE);
                    sorE.setProperty(CPIGraph.ID, id);
                } catch (Exception e) {
                    log.error("SOR addEdge Exception", e);
                }
            }
        });
    }

    void removeEdge(final String id) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Edge sorE = lookupEdge(id);
                    log.info("SOR removeEdge {} {}", id, sorE);
                    sor.removeEdge(sorE);
                } catch (Exception e) {
                    log.error("SOR removeEdge Exception", e);
                }
            }
        });
    }

    void setVProperty(final String id, final String key, final Object value) {
        executor.submit( new Runnable() {
            @Override
            public void run() {
                setProperty(lookupVertex(id), key, value);
            }
        });
    }
    void setEProperty(final String id, final String key, final Object value) {
        executor.submit( new Runnable() {
            @Override
            public void run() {
                setProperty(lookupEdge(id), key, value);
            }
        });
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
        executor.submit( new Runnable() {
            @Override
            public void run() {
                removeProperty(lookupVertex(id), key);
            }
        });
    }
    void removeEProperty(final String id, final String key) {
        executor.submit( new Runnable() {
            @Override
            public void run() {
                removeProperty(lookupEdge(id), key);
            }
        });
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
        executor.submit(new Runnable() {
            @Override
            public void run() {
                log.info("SOR commit");
                sor.commit();
            }
        });
    }

    void rollback() {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                log.info("SOR rollback");
                sor.rollback();
            }
        });
    }

    // =======================================
    private Vertex lookupVertex(String id) {
        try {
            return getOnlyElement(sor.getVertices(CPIGraph.ID, id));
        } catch (NoSuchElementException e) {
            log.error("No vertex found with id {}", id);
            throw e;
        } catch (IllegalArgumentException e) {
            log.error("Multiple vertices found with id {}", id);
            return getFirst(sor.getVertices(CPIGraph.ID, id), null);
        }
    }

    private Edge lookupEdge(String id) {
        try {
            return getOnlyElement(sor.getEdges(CPIGraph.ID, id));
        } catch (NoSuchElementException e) {
            log.error("No vertex found with id {}", id);
            throw e;
        } catch (IllegalArgumentException e) {
            log.error("Multiple edges found with id {}", id);
            return getFirst(sor.getEdges(CPIGraph.ID, id), null);
        }
    }

    // =======================================
    private final TransactionalGraph sor;
    private static final ExecutorService executor = Executors.newSingleThreadExecutor();
}
