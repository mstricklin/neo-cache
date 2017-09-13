// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package edu.utexas.arlut.ciads;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import edu.utexas.arlut.ciads.cpiGraph.CPIGraph;
import edu.utexas.arlut.ciads.cpiGraph.CPIGraphManager;
import edu.utexas.arlut.ciads.cpiGraph.CPIVertexProxy;
import edu.utexas.arlut.ciads.cpiGraph.SoRBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Map;

import static com.tinkerpop.blueprints.util.ElementHelper.getProperties;

@Slf4j
public class App {
    static class Neo4jBuilder implements SoRBuilder<Neo4jGraph> {
        Neo4jBuilder(String baseDir) {
            this.baseDir = baseDir;
        }

        @Override
        public Neo4jGraph build(String dir) {
            String path = baseDir + File.pathSeparatorChar + dir;
            Neo4jGraph g = new Neo4jGraph(path);
            g.createKeyIndex(CPIGraph.ID, Vertex.class);
            g.createKeyIndex(CPIGraph.ID, Edge.class);
            return g;
        }

        final String baseDir;
    }

    public static void main(String[] args) throws InterruptedException {


        final Neo4jGraph n4jg = new Neo4jGraph("graph");

        log.info("Neo contents");
        dumpGraph(n4jg);

        Neo4jBuilder builder = new Neo4jBuilder("graphs");
        CPIGraphManager cpiManager = new CPIGraphManager(builder);
        CPIGraph cg = cpiManager.create("aaa");
        log.info("Graph {}", cg);

        cg.createKeyIndex("foo", Vertex.class);

//        Vertex v0 = cg.getVertex("v0");
//        log.info("Got v0: {}", v0);
//        Vertex v1 = cg.getVertex("v1");
//        log.info("Got v1: {}", v1);

//        log.info("===");
//        cg.removeVertex(v0);
//        dumpGraph(cg);


//        Vertex v0 = cg.addVertex("v0");
//        v0.setProperty("foo", "v0");
//
//        Vertex v1 = cg.addVertex("v1");
//        v1.setProperty("foo", "v1");
//
//        Edge e0 = cg.addEdge("e0", v0, v1, "sam");
//        e0.setProperty("foo", "e1");

        log.info("= dump cpi =");
        dumpGraph(cg);
        Thread.sleep(500);
        log.info("= dump neo4j =");
        dumpGraph(n4jg);


//        cg.addEdge(null, v0, v1, "sam");


//        log.info("Cache contents");
//        dumpGraph(cg);
//        log.info("rollback contents");
//
//        cg.rollback();
//        dumpGraph(cg);
//
//        v0 = cg.addVertex(null);
//        v0.setProperty("foo", "v0");
//        log.info("commit contents");
//
        cg.commit();
//        dumpGraph(cg);


//        Thread.sleep(500);
//        log.info("");
//        log.info("Neo contents");
//        dumpGraph(n4jg);

//        cg.shutdown();
        cpiManager.shutdown();


//        n4jg.shutdown();
    }

    private static void dumpProperties(Element e) {
        for (Map.Entry<String, Object> me : getProperties(e).entrySet())
            log.info("\t{} => {}", me.getKey(), me.getValue());
    }

    public static void dumpGraph(Graph g) {
        for (Vertex v : g.getVertices()) {
            log.info("v: {}", v);
            dumpProperties(v);
        }
        for (Edge e : g.getEdges()) {
            log.info("e: {}", e);
            dumpProperties(e);
        }
    }
}
