/*
 * // CLASSIFICATION NOTICE: This file is UNCLASSIFIED
 */

package edu.utexas.arlut.ciads.suite;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.StringFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.*;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.size;
import static com.tinkerpop.blueprints.util.ElementHelper.getProperties;
import static org.junit.Assert.*;

@Slf4j
public class EdgeTestSuite extends TestSuite {


    @Test
    public void testEdgeEquality() {
        Vertex v = graph.addVertex(convertId("1"));
        Vertex u = graph.addVertex(convertId("2"));
        Edge e = graph.addEdge(null, v, u, convertLabel("knows"));
        assertEquals(e.getLabel(), convertLabel("knows"));
        assertEquals(e.getVertex(Direction.IN), u);
        assertEquals(e.getVertex(Direction.OUT), v);
        assertEquals(e, v.getEdges(Direction.OUT).iterator().next());
        assertEquals(e, u.getEdges(Direction.IN).iterator().next());
        assertEquals(v.getEdges(Direction.OUT).iterator().next(), u.getEdges(Direction.IN).iterator().next());
        Set<Edge> set = new HashSet<Edge>();
        set.add(e);
        set.add(e);
        set.add(v.getEdges(Direction.OUT).iterator().next());
        set.add(v.getEdges(Direction.OUT).iterator().next());
        set.add(u.getEdges(Direction.IN).iterator().next());
        set.add(u.getEdges(Direction.IN).iterator().next());
        if (graph.getFeatures().supportsEdgeIteration)
            set.add(graph.getEdges().iterator().next());
        assertEquals(set.size(), 1);
    }

    @Test
    public void testAddEdges() {
        Vertex v1 = graph.addVertex(convertId("1"));
        Vertex v2 = graph.addVertex(convertId("2"));
        Vertex v3 = graph.addVertex(convertId("3"));
        graph.addEdge(null, v1, v2, convertLabel("knows"));
        graph.addEdge(null, v2, v3, convertLabel("pets"));
        graph.addEdge(null, v2, v3, convertLabel("caresFor"));
        assertEquals(1, size(v1.getEdges(Direction.OUT)));
        assertEquals(2, size(v2.getEdges(Direction.OUT)));
        assertEquals(0, size(v3.getEdges(Direction.OUT)));
        assertEquals(0, size(v1.getEdges(Direction.IN)));
        assertEquals(1, size(v2.getEdges(Direction.IN)));
        assertEquals(2, size(v3.getEdges(Direction.IN)));
        printPerformance(graph.toString(), 6, "elements added and checked");
    }

    @Test
    public void testAddManyEdges() {
        int edgeCount = 100;
        int vertexCount = 200;
        long counter = 0l;
        resetAndStart();
        for (int i = 0; i < edgeCount; i++) {
            Vertex out = graph.addVertex(convertId("" + counter++));
            Vertex in = graph.addVertex(convertId("" + counter++));
            graph.addEdge(null, out, in, convertLabel(UUID.randomUUID().toString()));
        }
        printPerformance(graph.toString(), vertexCount + edgeCount, "elements added");
        if (graph.getFeatures().supportsEdgeIteration) {
            resetAndStart();
            assertEquals(edgeCount, size(graph.getEdges()));
            printPerformance(graph.toString(), edgeCount, "edges counted");
        }
        if (graph.getFeatures().supportsVertexIteration) {
            resetAndStart();
            assertEquals(vertexCount, size(graph.getVertices()));
            printPerformance(graph.toString(), vertexCount, "vertices counted");
            resetAndStart();
            for (Vertex vertex : graph.getVertices()) {
                if (size(vertex.getEdges(Direction.OUT)) > 0) {
                    assertEquals(1, size(vertex.getEdges(Direction.OUT)));
                    assertFalse(size(vertex.getEdges(Direction.IN)) > 0);

                } else {
                    assertEquals(1, size(vertex.getEdges(Direction.IN)));
                    assertFalse(size(vertex.getEdges(Direction.OUT)) > 0);
                }
            }
            printPerformance(graph.toString(), vertexCount, "vertices checked");
        }
    }

    @Test
    public void testGetEdges() {
        Vertex v1 = graph.addVertex(null);
        Vertex v2 = graph.addVertex(null);
        Vertex v3 = graph.addVertex(null);

        Edge e1 = graph.addEdge(null, v1, v2, convertLabel("test1"));
        Edge e2 = graph.addEdge(null, v2, v3, convertLabel("test2"));
        Edge e3 = graph.addEdge(null, v3, v1, convertLabel("test3"));

        if (graph.getFeatures().supportsEdgeRetrieval) {
            resetAndStart();
            assertEquals(graph.getEdge(e1.getId()), e1);
            assertEquals(graph.getEdge(e1.getId()).getVertex(Direction.IN), v2);
            assertEquals(graph.getEdge(e1.getId()).getVertex(Direction.OUT), v1);

            assertEquals(graph.getEdge(e2.getId()), e2);
            assertEquals(graph.getEdge(e2.getId()).getVertex(Direction.IN), v3);
            assertEquals(graph.getEdge(e2.getId()).getVertex(Direction.OUT), v2);

            assertEquals(graph.getEdge(e3.getId()), e3);
            assertEquals(graph.getEdge(e3.getId()).getVertex(Direction.IN), v1);
            assertEquals(graph.getEdge(e3.getId()).getVertex(Direction.OUT), v3);

            printPerformance(graph.toString(), 3, "edges retrieved");
        }

        assertEquals(getOnlyElement(v1.getEdges(Direction.OUT)), e1);
        assertEquals(getOnlyElement(v1.getEdges(Direction.OUT)).getVertex(Direction.IN), v2);
        assertEquals(getOnlyElement(v1.getEdges(Direction.OUT)).getVertex(Direction.OUT), v1);

        assertEquals(getOnlyElement(v2.getEdges(Direction.OUT)), e2);
        assertEquals(getOnlyElement(v2.getEdges(Direction.OUT)).getVertex(Direction.IN), v3);
        assertEquals(getOnlyElement(v2.getEdges(Direction.OUT)).getVertex(Direction.OUT), v2);

        assertEquals(getOnlyElement(v3.getEdges(Direction.OUT)), e3);
        assertEquals(getOnlyElement(v3.getEdges(Direction.OUT)).getVertex(Direction.IN), v1);
        assertEquals(getOnlyElement(v3.getEdges(Direction.OUT)).getVertex(Direction.OUT), v3);
    }

    @Test
    public void testGetNonExistantEdges() {
        if (graph.getFeatures().supportsEdgeRetrieval) {
            try {
                graph.getEdge(null);
                fail("Getting an element with a null identifier must throw IllegalArgumentException");
            } catch (IllegalArgumentException iae) {
                assertTrue(true);
            }

            assertNull(graph.getEdge("asbv"));
            assertNull(graph.getEdge(12.0d));
        }
    }

    @Test
    public void testRemoveManyEdges() {
        long counter = 200000l;
        int edgeCount = 10;
        Set<Edge> edges = new HashSet<Edge>();
        for (int i = 0; i < edgeCount; i++) {
            Vertex out = graph.addVertex(convertId("" + counter++));
            Vertex in = graph.addVertex(convertId("" + counter++));
            edges.add(graph.addEdge(null, out, in, convertLabel("a" + UUID.randomUUID().toString())));
        }
        assertEquals(edgeCount, edges.size());

        if (graph.getFeatures().supportsVertexIteration) {
            resetAndStart();
            assertEquals(edgeCount * 2, size(graph.getVertices()));
            printPerformance(graph.toString(), edgeCount * 2, "vertices counted");
        }

        if (graph.getFeatures().supportsEdgeIteration) {
            resetAndStart();
            assertEquals(edgeCount, size(graph.getEdges()));
            printPerformance(graph.toString(), edgeCount, "edges counted");

            Random random = new Random();
            int i = edgeCount;
            resetAndStart();
            for (Edge edge : edges) {
                if (random.nextBoolean())
                    graph.removeEdge(edge);
                else
                    edge.remove();
                i--;
                assertEquals(i, size(graph.getEdges()));
                if (graph.getFeatures().supportsVertexIteration) {
                    int x = 0;
                    for (Vertex vertex : graph.getVertices()) {
                        if (size(vertex.getEdges(Direction.OUT)) > 0) {
                            assertEquals(1, size(vertex.getEdges(Direction.OUT)));
                            assertFalse(size(vertex.getEdges(Direction.IN)) > 0);
                        } else if (size(vertex.getEdges(Direction.IN)) > 0) {
                            assertEquals(1, size(vertex.getEdges(Direction.IN)));
                            assertFalse(size(vertex.getEdges(Direction.OUT)) > 0);
                        } else {
                            x++;
                        }
                    }
                    assertEquals((edgeCount - i) * 2, x);
                }
            }
            printPerformance(graph.toString(), edgeCount, "edges removed and graph checked");
        }
    }

    @Test
    public void testAddingDuplicateEdges() {
        Vertex v1 = graph.addVertex(convertId("1"));
        Vertex v2 = graph.addVertex(convertId("2"));
        Vertex v3 = graph.addVertex(convertId("3"));
        graph.addEdge(null, v1, v2, convertLabel("knows"));
        graph.addEdge(null, v2, v3, convertLabel("pets"));
        graph.addEdge(null, v2, v3, convertLabel("pets"));
        graph.addEdge(null, v2, v3, convertLabel("pets"));
        graph.addEdge(null, v2, v3, convertLabel("pets"));

        if (graph.getFeatures().supportsDuplicateEdges) {
            if (graph.getFeatures().supportsVertexIteration)
                assertEquals(3, size(graph.getVertices()));
            if (graph.getFeatures().supportsEdgeIteration)
                assertEquals(5, size(graph.getEdges()));

            assertEquals(0, size(v1.getEdges(Direction.IN)));
            assertEquals(1, size(v1.getEdges(Direction.OUT)));
            assertEquals(1, size(v2.getEdges(Direction.IN)));
            assertEquals(4, size(v2.getEdges(Direction.OUT)));
            assertEquals(4, size(v3.getEdges(Direction.IN)));
            assertEquals(0, size(v3.getEdges(Direction.OUT)));
        } else {
            if (graph.getFeatures().supportsVertexIteration)
                assertEquals(size(graph.getVertices()), 3);
            if (graph.getFeatures().supportsEdgeIteration)
                assertEquals(size(graph.getEdges()), 2);

            assertEquals(0, size(v1.getEdges(Direction.IN)));
            assertEquals(1, size(v1.getEdges(Direction.OUT)));
            assertEquals(1, size(v2.getEdges(Direction.IN)));
            assertEquals(1, size(v2.getEdges(Direction.OUT)));
            assertEquals(1, size(v3.getEdges(Direction.IN)));
            assertEquals(0, size(v3.getEdges(Direction.OUT)));
        }
    }

    @Test
    public void testRemoveEdgesByRemovingVertex() {
        Vertex v1 = graph.addVertex(convertId("1"));
        Vertex v2 = graph.addVertex(convertId("2"));
        Vertex v3 = graph.addVertex(convertId("3"));
        graph.addEdge(null, v1, v2, convertLabel("knows"));
        graph.addEdge(null, v2, v3, convertLabel("pets"));
        graph.addEdge(null, v2, v3, convertLabel("pets"));

        assertEquals(0, size(v1.getEdges(Direction.IN)));
        assertEquals(1, size(v1.getEdges(Direction.OUT)));
        assertEquals(1, size(v2.getEdges(Direction.IN)));
        assertEquals(0, size(v3.getEdges(Direction.OUT)));

        if (!graph.getFeatures().ignoresSuppliedIds) {
            v1 = graph.getVertex(convertId("1"));
            v2 = graph.getVertex(convertId("2"));
            v3 = graph.getVertex(convertId("3"));

            assertEquals(0, size(v1.getEdges(Direction.IN)));
            assertEquals(1, size(v1.getEdges(Direction.OUT)));
            assertEquals(1, size(v2.getEdges(Direction.IN)));
            assertEquals(0, size(v3.getEdges(Direction.OUT)));
        }

        if (graph.getFeatures().supportsVertexIteration)
            assertEquals(3, size(graph.getVertices()));

        graph.removeVertex(v1);

        if (graph.getFeatures().supportsVertexIteration)
            assertEquals(2, size(graph.getVertices()));

        if (graph.getFeatures().supportsDuplicateEdges)
            assertEquals(2, size(v2.getEdges(Direction.OUT)));
        else
            assertEquals(1, size(v2.getEdges(Direction.OUT)));

        assertEquals(0, size(v3.getEdges(Direction.OUT)));
        assertEquals(0, size(v2.getEdges(Direction.IN)));

        if (graph.getFeatures().supportsDuplicateEdges)
            assertEquals(2, size(v3.getEdges(Direction.IN)));
        else
            assertEquals(1, size(v3.getEdges(Direction.IN)));
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
    @Test
    public void testRemoveEdges() {
        Vertex v1 = graph.addVertex(convertId("1"));
        Vertex v2 = graph.addVertex(convertId("2"));
        Vertex v3 = graph.addVertex(convertId("3"));
        Edge e1 = graph.addEdge(null, v1, v2, convertLabel("knows"));
        Edge e2 = graph.addEdge(null, v2, v3, convertLabel("pets"));
        Edge e3 = graph.addEdge(null, v2, v3, convertLabel("cares_for"));

        if (graph.getFeatures().supportsVertexIteration)
            assertEquals(3, size(graph.getVertices()));

        dumpGraph(graph);
        graph.removeEdge(e1);
        log.info("====");
        dumpGraph(graph);

        assertEquals(0, size(v1.getEdges(Direction.OUT)));
        assertEquals(2, size(v2.getEdges(Direction.OUT)));
        assertEquals(0, size(v3.getEdges(Direction.OUT)));
        assertEquals(0, size(v1.getEdges(Direction.IN)));
        assertEquals(0, size(v2.getEdges(Direction.IN)));
        assertEquals(2, size(v3.getEdges(Direction.IN)));
        if (!graph.getFeatures().ignoresSuppliedIds) {
            v1 = graph.getVertex(convertId("1"));
            v2 = graph.getVertex(convertId("2"));
            v3 = graph.getVertex(convertId("3"));
        }
        assertEquals(0, size(v1.getEdges(Direction.OUT)));
        assertEquals(2, size(v2.getEdges(Direction.OUT)));
        assertEquals(0, size(v3.getEdges(Direction.OUT)));
        assertEquals(0, size(v1.getEdges(Direction.IN)));
        assertEquals(0, size(v2.getEdges(Direction.IN)));
        assertEquals(2, size(v3.getEdges(Direction.IN)));

        e2.remove();
        assertEquals(0, size(v1.getEdges(Direction.OUT)));
        assertEquals(1, size(v2.getEdges(Direction.OUT)));
        assertEquals(0, size(v3.getEdges(Direction.OUT)));
        assertEquals(0, size(v1.getEdges(Direction.IN)));
        assertEquals(0, size(v2.getEdges(Direction.IN)));
        assertEquals(1, size(v3.getEdges(Direction.IN)));
        if (!graph.getFeatures().ignoresSuppliedIds) {
            v1 = graph.getVertex(convertId("1"));
            v2 = graph.getVertex(convertId("2"));
            v3 = graph.getVertex(convertId("3"));
        }
        assertEquals(0, size(v1.getEdges(Direction.OUT)));
        assertEquals(1, size(v2.getEdges(Direction.OUT)));
        assertEquals(0, size(v3.getEdges(Direction.OUT)));
        assertEquals(0, size(v1.getEdges(Direction.IN)));
        assertEquals(0, size(v2.getEdges(Direction.IN)));
        assertEquals(1, size(v3.getEdges(Direction.IN)));

        graph.removeEdge(e3);
        assertEquals(0, size(v1.getEdges(Direction.OUT)));
        assertEquals(0, size(v2.getEdges(Direction.OUT)));
        assertEquals(0, size(v3.getEdges(Direction.OUT)));
        assertEquals(0, size(v1.getEdges(Direction.IN)));
        assertEquals(0, size(v2.getEdges(Direction.IN)));
        assertEquals(0, size(v3.getEdges(Direction.IN)));
        if (!graph.getFeatures().ignoresSuppliedIds) {
            v1 = graph.getVertex(convertId("1"));
            v2 = graph.getVertex(convertId("2"));
            v3 = graph.getVertex(convertId("3"));
        }
        assertEquals(0, size(v1.getEdges(Direction.OUT)));
        assertEquals(0, size(v2.getEdges(Direction.OUT)));
        assertEquals(0, size(v3.getEdges(Direction.OUT)));
        assertEquals(0, size(v1.getEdges(Direction.IN)));
        assertEquals(0, size(v2.getEdges(Direction.IN)));
        assertEquals(0, size(v3.getEdges(Direction.IN)));
    }

    @Test
    public void testAddingSelfLoops() {
        if (graph.getFeatures().supportsSelfLoops) {
            Vertex v1 = graph.addVertex(convertId("1"));
            Vertex v2 = graph.addVertex(convertId("2"));
            Vertex v3 = graph.addVertex(convertId("3"));
            graph.addEdge(null, v1, v1, convertLabel("is_self"));
            graph.addEdge(null, v2, v2, convertLabel("is_self"));
            graph.addEdge(null, v3, v3, convertLabel("is_self"));

            if (graph.getFeatures().supportsVertexIteration)
                assertEquals(3, size(graph.getVertices()));
            if (graph.getFeatures().supportsEdgeIteration) {
                assertEquals(3, size(graph.getEdges()));
                int counter = 0;
                for (Edge edge : graph.getEdges()) {
                    counter++;
                    assertEquals(edge.getVertex(Direction.IN), edge.getVertex(Direction.OUT));
                    assertEquals(edge.getVertex(Direction.IN).getId(), edge.getVertex(Direction.OUT).getId());
                }
                assertEquals(counter, 3);
            }
        }
    }

    @Test
    public void testRemoveSelfLoops() {
        if (graph.getFeatures().supportsSelfLoops) {
            Vertex v1 = graph.addVertex(convertId("1"));
            Vertex v2 = graph.addVertex(convertId("2"));
            Vertex v3 = graph.addVertex(convertId("3"));
            Edge e1 = graph.addEdge(null, v1, v1, convertLabel("is_self"));
            Edge e2 = graph.addEdge(null, v2, v2, convertLabel("is_self"));
            Edge e3 = graph.addEdge(null, v3, v3, convertLabel("is_self"));

            if (graph.getFeatures().supportsVertexIteration)
                assertEquals(3, size(graph.getVertices()));
            if (graph.getFeatures().supportsEdgeIteration) {
                assertEquals(3, size(graph.getEdges()));
                for (Edge edge : graph.getEdges()) {
                    assertEquals(edge.getVertex(Direction.IN), edge.getVertex(Direction.OUT));
                    assertEquals(edge.getVertex(Direction.IN).getId(), edge.getVertex(Direction.OUT).getId());
                }
            }

            graph.removeVertex(v1);
            if (graph.getFeatures().supportsEdgeIteration) {
                assertEquals(2, size(graph.getEdges()));
                for (Edge edge : graph.getEdges()) {
                    assertEquals(edge.getVertex(Direction.IN), edge.getVertex(Direction.OUT));
                    assertEquals(edge.getVertex(Direction.IN).getId(), edge.getVertex(Direction.OUT).getId());
                }
            }

            assertEquals(1, size(v2.getEdges(Direction.OUT)));
            assertEquals(1, size(v2.getEdges(Direction.IN)));
            graph.removeEdge(e2);
            assertEquals(0, size(v2.getEdges(Direction.OUT)));
            assertEquals(0, size(v2.getEdges(Direction.IN)));

            if (graph.getFeatures().supportsEdgeIteration) {
                assertEquals(size(graph.getEdges()), 1);
                for (Edge edge : graph.getEdges()) {
                    assertEquals(edge.getVertex(Direction.IN), edge.getVertex(Direction.OUT));
                    assertEquals(edge.getVertex(Direction.IN).getId(), edge.getVertex(Direction.OUT).getId());
                }
            }
        }
    }

    @Test
    public void testEdgeIterator() {
        if (graph.getFeatures().supportsEdgeIteration) {
            Vertex v1 = graph.addVertex(convertId("1"));
            Vertex v2 = graph.addVertex(convertId("2"));
            Vertex v3 = graph.addVertex(convertId("3"));
            Edge e1 = graph.addEdge(null, v1, v2, convertLabel("test"));
            Edge e2 = graph.addEdge(null, v2, v3, convertLabel("test"));
            Edge e3 = graph.addEdge(null, v3, v1, convertLabel("test"));

            if (graph.getFeatures().supportsVertexIteration)
                assertEquals(3, size(graph.getVertices()));
            if (graph.getFeatures().supportsEdgeIteration)
                assertEquals(3, size(graph.getEdges()));

            Set<String> edgeIds = new HashSet<String>();
            int count = 0;
            for (Edge e : graph.getEdges()) {
                count++;
                edgeIds.add(e.getId().toString());
                assertEquals(convertId("test"), e.getLabel());
                if (e.getId().toString().equals(e1.getId().toString())) {
                    assertEquals(v1, e.getVertex(Direction.OUT));
                    assertEquals(v2, e.getVertex(Direction.IN));
                } else if (e.getId().toString().equals(e2.getId().toString())) {
                    assertEquals(v2, e.getVertex(Direction.OUT));
                    assertEquals(v3, e.getVertex(Direction.IN));
                } else if (e.getId().toString().equals(e3.getId().toString())) {
                    assertEquals(v3, e.getVertex(Direction.OUT));
                    assertEquals(v1, e.getVertex(Direction.IN));
                } else {
                    assertTrue(false);
                }
                //System.out.println(e);
            }
            assertEquals(3, count);
            assertEquals(3, edgeIds.size());
            assertTrue(edgeIds.contains(e1.getId().toString()));
            assertTrue(edgeIds.contains(e2.getId().toString()));
            assertTrue(edgeIds.contains(e3.getId().toString()));
        }
    }

    @Test
    public void testAddingRemovingEdgeProperties() {
        if (graph.getFeatures().supportsEdgeProperties) {
            Vertex a = graph.addVertex(convertId("1"));
            Vertex b = graph.addVertex(convertId("2"));
            Edge edge = graph.addEdge(convertId("3"), a, b, "knows");
            assertEquals(edge.getPropertyKeys().size(), 0);
            assertNull(edge.getProperty("weight"));

            if (graph.getFeatures().supportsDoubleProperty) {
                edge.setProperty("weight", 0.5);
                assertEquals(edge.getPropertyKeys().size(), 1);
                assertEquals(edge.getProperty("weight"), 0.5);

                edge.setProperty("weight", 0.6);
                assertEquals(edge.getPropertyKeys().size(), 1);
                assertEquals(edge.getProperty("weight"), 0.6);
                assertEquals(edge.removeProperty("weight"), 0.6);
                assertNull(edge.getProperty("weight"));
                assertEquals(edge.getPropertyKeys().size(), 0);
            }

            if (graph.getFeatures().supportsStringProperty) {
                edge.setProperty("blah", "marko");
                edge.setProperty("blah2", "josh");
                assertEquals(edge.getPropertyKeys().size(), 2);
            }
        }
    }

    @Test
    public void testAddingLabelAndIdProperty() {

        // no point in testing graph features for setting string properties because the intent is for it to
        // fail based on the key or label properties.
        if (graph.getFeatures().supportsEdgeProperties) {

            Edge edge = graph.addEdge(null, graph.addVertex(null), graph.addVertex(null), "knows");
            try {
                edge.setProperty("id", "123");
                fail("Setting edge property with reserved key 'key' should fail");
            } catch (RuntimeException e) {
            }
            try {
                edge.setProperty("label", "hates");
                fail("Setting edge property with reserved key 'label' should fail");
            } catch (RuntimeException e) {
            }

        }
    }

    @Test
    public void testNoConcurrentModificationException() {
        for (int i = 0; i < 25; i++) {
            graph.addEdge(null, graph.addVertex(null), graph.addVertex(null), convertLabel("test"));
        }
        if (graph.getFeatures().supportsVertexIteration)
            assertEquals(size(graph.getVertices()), 50);
        if (graph.getFeatures().supportsEdgeIteration) {
            assertEquals(size(graph.getEdges()), 25);
            for (final Edge edge : graph.getEdges()) {
                graph.removeEdge(edge);
            }
            assertEquals(size(graph.getEdges()), 0);
        }
        if (graph.getFeatures().supportsVertexIteration)
            assertEquals(size(graph.getVertices()), 50);
    }

    @Test
    public void testEmptyKeyProperty() {

        // no point in testing graph features for setting string properties because the intent is for it to
        // fail based on the empty key.
        if (graph.getFeatures().supportsEdgeProperties) {
            final Edge e = graph.addEdge(null, graph.addVertex(null), graph.addVertex(null), "friend");
            try {
                e.setProperty("", "value");
                fail("Setting an edge property with an empty string key should fail");
            } catch (IllegalArgumentException e1) {
            }
        }
    }

    @Test
    public void testEdgeCentricRemoving() {

        final Edge a = graph.addEdge(null, graph.addVertex(null), graph.addVertex(null), convertLabel("knows"));
        final Edge b = graph.addEdge(null, graph.addVertex(null), graph.addVertex(null), convertLabel("knows"));
        final Edge c = graph.addEdge(null, graph.addVertex(null), graph.addVertex(null), convertLabel("knows"));

        Object cId = c.getId();

        if (graph.getFeatures().supportsEdgeIteration)
            assertEquals(size(graph.getEdges()), 3);

        a.remove();
        b.remove();

        if (graph.getFeatures().supportsEdgeRetrieval)
            assertNotNull(graph.getEdge(cId));

        if (graph.getFeatures().supportsEdgeIteration)
            assertEquals(size(graph.getEdges()), 1);

    }

    @Test
    public void testSettingBadVertexProperties() {
        if (graph.getFeatures().supportsVertexProperties) {
            Edge edge = graph.addEdge(null, graph.addVertex(null), graph.addVertex(null), "knows");
            try {
                edge.setProperty(null, -1);
                fail("Setting property with a null key should throw an error");
            } catch (RuntimeException e) {
            }
            try {
                edge.setProperty("", -1);
                fail("Setting edge property with a empty key should throw an error");
            } catch (RuntimeException e) {
            }
            try {
                edge.setProperty(StringFactory.ID, -1);
                fail("Setting edge property with a key of '" + StringFactory.ID +
                        "' should throw an error");
            } catch (RuntimeException e) {
            }
            try {
                edge.setProperty(StringFactory.LABEL, "friend");
                fail("Setting edge property with a key of '" + StringFactory.LABEL +
                        "' should throw an error");
            } catch (RuntimeException e) {
            }
            try {
                edge.setProperty("good", null);
                fail("Setting edge property with a null value should throw an error");
            } catch (RuntimeException e) {
            }
        }
    }

    @Test
    public void testSetEdgeLabelNullShouldThrowIllegalArgumentException() {
        Vertex v1 = graph.addVertex(null);
        Vertex v2 = graph.addVertex(null);

        try {
            v2.addEdge(null, v1);
            fail("Adding edge to vertex with a null label should throw an error");
        } catch (IllegalArgumentException iae) {
        }

        try {
            graph.addEdge(null, v1, v2, null);
            fail("Adding edge to graph with a null label should throw an error");
        } catch (IllegalArgumentException iae) {
        }
    }
}
