// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package edu.utexas.arlut.ciads;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import edu.utexas.arlut.ciads.cpiGraph.CPIGraph;
import edu.utexas.arlut.ciads.cpiGraph.CPIGraphManager;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.tinkerpop.blueprints.util.ElementHelper.getProperties;

@Slf4j
public class App {
    public static void main(String[] args) throws InterruptedException {

        final Neo4jGraph n4jg = new Neo4jGraph("graph");

        log.info("Neo contents");
        dumpGraph(n4jg);

        CPIGraphManager cpiManager = new CPIGraphManager(n4jg);
        CPIGraph cg = cpiManager.create("aaa");
        log.info("Graph {}", cg);

        cg.createKeyIndex("foo", Vertex.class);
        Vertex v0 = cg.addVertex(null);
        v0.setProperty("foo", "v0");

        Vertex v1 = cg.addVertex(null);
        v1.setProperty("foo", "v1");

//        cg.addEdge(null, v0, v1, "sam");


        log.info("Cache contents");
        dumpGraph(cg);
        log.info("rollback contents");

        cg.rollback();
        dumpGraph(cg);

        v0 = cg.addVertex(null);
        v0.setProperty("foo", "v0");
        log.info("commit contents");

        cg.commit();
        dumpGraph(cg);



//        Thread.sleep(500);
//        log.info("");
//        log.info("Neo contents");
//        dumpGraph(n4jg);

//        cg.shutdown();
        cpiManager.shutdown();


//        n4jg.shutdown();

    }

    private static void dumpProperties(Element e) {
        for (Map.Entry<String, Object> me: getProperties(e).entrySet())
            log.info("\t{} => {}", me.getKey(), me.getValue());
    }
    public static void dumpGraph(Graph g) {
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
