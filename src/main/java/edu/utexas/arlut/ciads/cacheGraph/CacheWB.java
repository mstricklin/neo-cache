package edu.utexas.arlut.ciads.cacheGraph;

import com.tinkerpop.blueprints.*;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class CacheWB<T extends TransactionalGraph & KeyIndexableGraph> extends WriteBehind {

    CacheWB(T writeTo) {
        this.writeTo = writeTo;
    }
    public void shutdown() {
        executor.shutdown();
    }

    void addVertex(final CachedVertex proxy) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                Vertex v = writeTo.addVertex(null);
                proxy.setBase(v);
                v.setProperty(CachedGraph.ID, proxy.getId());
                writeTo.commit();
                log.info("Executor addVertex {}", v);
            }
        });
    }
    void removeVertex(final CachedVertex proxy) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                proxy.getBaseVertex().remove();
                writeTo.commit();
            }
        });
    }
    void addEdge(final CachedEdge proxy) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                Vertex oV = proxy.startVertex.getBaseVertex();
                Vertex iV = proxy.endVertex.getBaseVertex();
                Edge e = writeTo.addEdge(null, oV, iV, proxy.label);
                proxy.setBase(e);
                e.setProperty(CachedGraph.ID, proxy.getId());
                writeTo.commit();
                log.info("Executor addEdge {}", e);
            }
        });
    }
    void removeEdge(final CachedEdge proxy) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                proxy.getBaseEdge().remove();
                writeTo.commit();
            }
        });
    }
    void setProperty(final CachedElement proxy, final String key, final Object val) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                proxy.getBase().setProperty(key, val);
                writeTo.commit();
                log.info("Executor setProperty {} {}", key, val);

            }
        });
    }
    void removeProperty(final CachedElement proxy, final String key) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                proxy.getBase().removeProperty(key);
                writeTo.commit();
            }
        });
    }
    void addKeyIndex(final String key, final Class<? extends Element> clazz) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                writeTo.createKeyIndex(key, clazz);
                writeTo.commit();
                log.info("Executor addKeyIndex {}|{}",clazz.getSimpleName(), key);

            }
        });
    }
    void dropKeyIndex(final String key, final Class<? extends Element> clazz) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                writeTo.dropKeyIndex(key, clazz);
                writeTo.commit();
                log.info("Executor dropKeyIndex {}|{}",clazz.getSimpleName(), key);
            }
        });
    }
    private final T writeTo;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
}
