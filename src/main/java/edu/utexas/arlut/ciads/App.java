// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package edu.utexas.arlut.ciads;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.tinkerpop.blueprints.util.ElementHelper.getProperties;

@Slf4j
public class App {
    public static void main(String[] args) throws InterruptedException {
        final Neo4jGraph n4jg = new Neo4jGraph("graph");

        CacheGraph<Neo4jGraph> cg = new CacheGraph<>(n4jg);
        log.info("Graph {}", cg);

        Vertex v0 = cg.addVertex(null);
        Vertex v1 = cg.addVertex(null);
        cg.addEdge(null, v0, v1, "sam");

        cg.createKeyIndex("foo", Vertex.class);

        log.info("Cache contents");
        dumpGraph(cg);

        Thread.sleep(500);
        log.info("");
        log.info("Neo contents");
        dumpGraph(n4jg);

        cg.shutdown();
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