package edu.utexas.arlut.ciads.suite;

import com.google.common.base.Stopwatch;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Collection;

import static com.google.common.collect.Iterables.size;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Slf4j
public class GraphUtil {


    protected void vertexCount(final Graph graph, int expectedCount) {
        if (graph.getFeatures().supportsVertexIteration) assertEquals(size(graph.getVertices()), expectedCount);
    }

    protected void containsVertices(final Graph graph, final Collection<Vertex> vertices) {
        for (Vertex v : vertices) {
            Vertex vp = graph.getVertex(v.getId());
            if (vp == null || !vp.getId().equals(v.getId()))
                fail("Graph does not contain vertex: '" + v + "'");
        }
    }

    protected void edgeCount(final Graph graph, int expectedCount) {
        if (graph.getFeatures().supportsEdgeIteration) assertEquals(size(graph.getEdges()), expectedCount);
    }


    protected static void deleteDirectory(final File directory) {
        if (directory.exists()) {
            for (File file : directory.listFiles()) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
            directory.delete();
        }

        // overkill code, simply allowing us to detect when data dir is in use.  useful though because without it
        // tests may fail if a database is re-used in between tests somehow.  this directory really needs to be
        // cleared between tests runs and this exception will make it clear if it is not.
        if (directory.exists()) {
            throw new RuntimeException("unable to delete directory " + directory.getAbsolutePath());
        }
    }
}
