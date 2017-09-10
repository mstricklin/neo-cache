/*
 * // CLASSIFICATION NOTICE: This file is UNCLASSIFIED
 */

package edu.utexas.arlut.ciads.suite;

import com.google.common.base.Stopwatch;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import edu.utexas.arlut.ciads.cpiGraph.CPIGraph;
import edu.utexas.arlut.ciads.cpiGraph.CPIGraphManager;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.Collection;

import static com.google.common.collect.Iterables.size;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Slf4j
public class TestSuite {

    @Rule
    public TestName name = new TestName();
    public Stopwatch sw = Stopwatch.createUnstarted();

    protected CPIGraphManager factory = new CPIGraphManager(null);
    protected Graph graph = null;
    protected CPIGraph gg = null;

    CPIGraph generateGraph() throws  IOException {
        return factory.getGraph("aaa");
    }

    @Before
    public void before() throws  IOException {
//        deleteDirectory(new File(workingDir));
        log.info("Testing {}...", name.getMethodName());
        graph = generateGraph();
        gg = (CPIGraph)graph;
//        sw = Stopwatch.createUnstarted();
        Stopwatch.createStarted();
    }

    @After
    public void after() {
        graph.shutdown();
        sw.reset();
        log.info("*** TOTAL TIME [{}]: {} ***", name.getMethodName(), sw.toString());
//        deleteDirectory(new File(workingDir));
    }

    public static Object convertId(final Object id) {
        return id;
    }

    public static String convertLabel(final String label) {
        return label;
    }

    protected void resetAndStart() {
        sw.reset();
        sw.start();
    }

    protected void printPerformance(String name, Integer events, String eventName) {
        //sw.stop();
        if (null != events) {
            log.info("\t{}: {} {} in {}ms", name, events, eventName, sw.elapsed(MILLISECONDS));
        } else {
            log.info("\t{}: {} in {}ms", name, eventName, sw.elapsed(MILLISECONDS));
        }
    }

    protected void vertexCount(final Graph graph, int expectedCount) {
        if (graph.getFeatures().supportsVertexIteration) assertEquals(size(graph.getVertices()), expectedCount);
    }

    protected void edgeCount(final Graph graph, int expectedCount) {
        if (graph.getFeatures().supportsEdgeIteration) assertEquals(size(graph.getEdges()), expectedCount);
    }

    protected void containsVertices(final Graph graph, final Collection<Vertex> vertices) {
        for (Vertex v : vertices) {
            Vertex vp = graph.getVertex(v.getId());
            if (vp == null || !vp.getId().equals(v.getId()))
                fail("Graph does not contain vertex: '" + v + "'");
        }
    }

}
