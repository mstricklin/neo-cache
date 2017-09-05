// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package edu.utexas.arlut.ciads;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import edu.utexas.arlut.ciads.cacheGraph.CachedGraph;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static com.tinkerpop.blueprints.util.ElementHelper.getProperties;

@Slf4j
public class App {
    public static void main(String[] args) throws InterruptedException {

        final Neo4jGraph n4jg = new Neo4jGraph("graph");

        log.info("Neo contents");
        dumpGraph(n4jg);

        CachedGraph<Neo4jGraph> cg = new CachedGraph<>(n4jg);
        log.info("Graph {}", cg);

        cg.createKeyIndex("foo", Vertex.class);
        Vertex v0 = cg.addVertex(null);
        v0.setProperty("foo", "v0");
        Vertex v1 = cg.addVertex(null);
        v1.setProperty("foo", "v1");

        cg.addEdge(null, v0, v1, "sam");


        log.info("Cache contents");
        dumpGraph(cg);

        Thread.sleep(500);
        log.info("");
        log.info("Neo contents");
        dumpGraph(n4jg);

        cg.shutdown();

        Map<String, String> a = newHashMap();
        a.put("a", "aaa0");
        a.put("b", "bbb0");

        Map<String, String> b = newHashMap();
        b.put("a", "aaa1");
        b.put("c", "ccc1");
        for (Map.Entry<String, String> me: a.entrySet())
            log.info("a: {}", me);
        log.info("");
        a.putAll(b);
        for (Map.Entry<String, String> me: a.entrySet())
            log.info("a: {}", me);

    }

    private static void dumpProperties(Element e) {
        for (Map.Entry<String, Object> me: getProperties(e).entrySet())
            log.info("\t{} => {}", me.getKey(), me.getValue());
    }
    private static void dumpGraph(Graph g) {
        for (Vertex v: g.getVertices()) {
            log.info("v: {}", v);
            dumpProperties(v);
        }
        for (Edge e: g.getEdges()) {
            log.info("e: {}", e);
            dumpProperties(e);
        }
    }
}
