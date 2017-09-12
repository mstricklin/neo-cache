package edu.utexas.arlut.ciads;

import com.google.common.base.Stopwatch;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import edu.utexas.arlut.ciads.cpiGraph.CPIGraph;
import edu.utexas.arlut.ciads.cpiGraph.CPIGraphManager;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.net.URL;

@Slf4j
public class CPITest {
    @Rule
    public TestName name = new TestName();
    @Rule
    public final TemporaryFolder tmpDir = new TemporaryFolder();
    public Stopwatch sw;
    CPIGraphManager cpiManager;
    Neo4jGraph n4jg;
    CPIGraph graph;


    @Test
    public void test00() {
        Vertex v0 = graph.addVertex("v0");
        v0.setProperty("foo", "v0");

        Vertex v1 = graph.addVertex("v1");
        v1.setProperty("foo", "v1");

        Edge e0 = graph.addEdge("e0", v0, v1, "sam");
        e0.setProperty("foo", "e1");

    }

    @Test
    public void testContend() throws InterruptedException {
        Vertex v0 = graph.addVertex("v0");
        v0.setProperty("aaa", "one");
        graph.commit();
        for (int i = 1; i < 10; i++) {
            graph.addVertex("v" + i);
        }
        graph.commit();
        Thread.sleep(500);
        dump(n4jg);

        for (int i = 1; i < 10; i++) {
            Vertex v = graph.getVertex("v" + i);
            v.setProperty("aaa", "a1");
        }
        graph.commit();
        v0.setProperty("aaa", "two");
        graph.commit();
        Thread.sleep(500);
        dump(n4jg);

//        graph

//        Edge e0 = graph.addEdge("e0", v0, v1, "sam");
//        e0.setProperty("foo", "e1");

    }

    @Test
    public void testRmVertex() throws InterruptedException {
        Vertex v0 = graph.addVertex("v0");
        v0.setProperty("aaa", "one");
        graph.commit();
        Thread.sleep(500);
        dump(n4jg);
        graph.removeVertex(v0);
        graph.commit();
        Thread.sleep(500);
        dump(n4jg);
    }

    @Test
    public void testAddEdge() throws InterruptedException {
        Vertex v0 = graph.addVertex("v0");
        Vertex v1 = graph.addVertex("v1");
        graph.addEdge("e0", v0, v1, "sam");
        graph.commit();
        Thread.sleep(500);
        dump(n4jg);
    }

    @Test
    public void testAddRmProps() throws InterruptedException {
        Vertex v0 = graph.addVertex("v0");
        Vertex v1 = graph.addVertex("v1");
        Edge e0 = graph.addEdge("e0", v0, v1, "sam");
        v0.setProperty("vertex prop0", Integer.valueOf(0));
        e0.setProperty("edge prop0", Integer.valueOf(0));
        graph.commit();
        Thread.sleep(500);
        dump(n4jg);

        log.info(" === ");
        v0.removeProperty("vertex prop0");
        e0.removeProperty("edge prop0");
        graph.commit();
        Thread.sleep(500);
        dump(n4jg);
    }

    @Test
    public void testCreateFromBaseline() throws InterruptedException {
        Vertex v0 = graph.addVertex("v0");
        Vertex v1 = graph.addVertex("v1");
        Edge e0 = graph.addEdge("e0", v0, v1, "sam");
        v0.setProperty("vertex prop0", Integer.valueOf(0));
        e0.setProperty("edge prop0", Integer.valueOf(0));
        graph.commit();
        Thread.sleep(500);
        dump(n4jg);
        log.info(" ===== ");
        CPIGraph graph2 = cpiManager.createFrom(graph, "bbb");
        graph.commit();
        Thread.sleep(500);
        dump(n4jg);
    }

    @Before
    public void before() throws IOException {
        File graphDir = tmpDir.newFolder("graph");
        log.info("graph dir {}", graphDir);
        n4jg = new Neo4jGraph(graphDir.getPath());

        cpiManager = new CPIGraphManager(n4jg);
        graph = cpiManager.create("aaa");
        log.info("Testing {}...", name.getMethodName());
        sw = Stopwatch.createStarted();
    }

    void dump(Element e) {
        for (String k : e.getPropertyKeys())
            log.info("\t{} => {}", k, e.getProperty(k));
    }

    void dump(Graph g) {
        for (Vertex v : g.getVertices()) {
            log.info(v.toString());
            dump(v);
        }
        for (Edge e : g.getEdges()) {
            log.info(e.toString());
            dump(e);
        }
    }

    @After
    public void after() {
        sw.stop();
        log.info("*** TOTAL TIME [{}]: {} ***", name.getMethodName(), sw.toString());
        cpiManager.shutdown();
    }

    public File computeTestDataRoot() {
        final String clsUri = this.getClass().getName().replace('.', '/') + ".class";
        final URL url = this.getClass().getClassLoader().getResource(clsUri);
        final String clsPath = url.getPath();
        final File root = new File(clsPath.substring(0, clsPath.length() - clsUri.length()));
        return new File(root.getParentFile(), "test-data");
    }
}
