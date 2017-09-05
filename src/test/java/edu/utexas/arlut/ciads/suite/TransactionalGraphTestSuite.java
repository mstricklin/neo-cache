package edu.utexas.arlut.ciads.suite;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.size;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.*;

@Slf4j
public class TransactionalGraphTestSuite extends TestSuite {

    @Test
    public void testRepeatedTransactionStopException() {
        TransactionalGraph tgraph = (TransactionalGraph) graph;
        tgraph.commit();
        tgraph.rollback();
        tgraph.commit();
    }

    @Test
    public void testAutoStartTransaction() {
        TransactionalGraph tgraph = (TransactionalGraph) graph;
        Vertex v1 = tgraph.addVertex(null);
        vertexCount(tgraph, 1);
        assertEquals(v1.getId(), tgraph.getVertex(v1.getId()).getId());
        tgraph.commit();
        vertexCount(tgraph, 1);
        assertEquals(v1.getId(), tgraph.getVertex(v1.getId()).getId());
    }


    @Test
    public void testTransactionsForVertices() {
        TransactionalGraph tgraph = (TransactionalGraph) graph;
        List<Vertex> vin = new ArrayList<Vertex>();
        List<Vertex> vout = new ArrayList<Vertex>();
        vin.add(tgraph.addVertex(null));
        tgraph.commit();
        vertexCount(tgraph, 1);
        containsVertices(tgraph, vin);

        resetAndStart();
        vout.add(tgraph.addVertex(null));
        vertexCount(tgraph, 2);
        containsVertices(tgraph, vin);
        containsVertices(tgraph, vout);
        tgraph.rollback();

        containsVertices(tgraph, vin);
        vertexCount(tgraph, 1);
        printPerformance(tgraph.toString(), 1, "vertex not added in failed transaction");

        resetAndStart();
        vin.add(tgraph.addVertex(null));
        vertexCount(tgraph, 2);
        containsVertices(tgraph, vin);
        tgraph.commit();
        printPerformance(tgraph.toString(), 1, "vertex added in successful transaction");
        vertexCount(tgraph, 2);
        containsVertices(tgraph, vin);
    }

    @Test
    public void testBasicVertexEdgeTransactions() {
        TransactionalGraph tgraph = (TransactionalGraph) graph;
        Vertex v = tgraph.addVertex(null);
        tgraph.addEdge(null, v, v, convertLabel("self"));
        assertEquals(size(v.getEdges(Direction.IN)), 1);
        assertEquals(size(v.getEdges(Direction.OUT)), 1);
        assertEquals(v.getEdges(Direction.IN).iterator().next(), v.getEdges(Direction.OUT).iterator().next());
        tgraph.commit();
        v = tgraph.getVertex(v.getId());
        assertEquals(size(v.getEdges(Direction.IN)), 1);
        assertEquals(size(v.getEdges(Direction.OUT)), 1);
        assertEquals(v.getEdges(Direction.IN).iterator().next(), v.getEdges(Direction.OUT).iterator().next());
        tgraph.commit();
        v = tgraph.getVertex(v.getId());
        assertEquals(size(v.getVertices(Direction.IN)), 1);
        assertEquals(size(v.getVertices(Direction.OUT)), 1);
        assertEquals(v.getVertices(Direction.IN).iterator().next(), v.getVertices(Direction.OUT).iterator().next());
        tgraph.commit();
    }

    @Test
    public void testBruteVertexTransactions() {
        TransactionalGraph tgraph = (TransactionalGraph) graph;
        List<Vertex> vin = new ArrayList<Vertex>(), vout = new ArrayList<Vertex>();
        resetAndStart();
        for (int i = 0; i < 100; i++) {
            vin.add(tgraph.addVertex(null));
            tgraph.commit();
        }
        printPerformance(tgraph.toString(), 100, "vertices added in 100 successful transactions");
        vertexCount(tgraph, 100);
        containsVertices(tgraph, vin);

        resetAndStart();
        for (int i = 0; i < 100; i++) {
            vout.add(tgraph.addVertex(null));
            tgraph.rollback();
        }
        printPerformance(tgraph.toString(), 100, "vertices not added in 100 failed transactions");

        vertexCount(tgraph, 100);
        containsVertices(tgraph, vin);
        tgraph.rollback();
        vertexCount(tgraph, 100);
        containsVertices(tgraph, vin);


        resetAndStart();
        for (int i = 0; i < 100; i++) {
            vin.add(tgraph.addVertex(null));
        }
        vertexCount(tgraph, 200);
        log.info("found 200: {}", size(graph.getVertices()));
        containsVertices(tgraph, vin);
        tgraph.commit();
        printPerformance(tgraph.toString(), 100, "vertices added in 1 successful transactions");
        log.info("found 200: {}", size(graph.getVertices()));
        vertexCount(tgraph, 200);
        containsVertices(tgraph, vin);

        resetAndStart();
        for (int i = 0; i < 100; i++) {
            vout.add(tgraph.addVertex(null));
        }
        vertexCount(tgraph, 300);
        containsVertices(tgraph, vin);
        containsVertices(tgraph, vout.subList(100, 200));
        tgraph.rollback();
        printPerformance(tgraph.toString(), 100, "vertices not added in 1 failed transactions");
        vertexCount(tgraph, 200);
        containsVertices(tgraph, vin);
    }

    @Test
    public void testTransactionsForEdges() {
        TransactionalGraph tgraph = (TransactionalGraph) graph;

        Vertex v = tgraph.addVertex(null);
        Vertex u = tgraph.addVertex(null);
        tgraph.commit();

        resetAndStart();
        Edge e = tgraph.addEdge(null, tgraph.getVertex(v.getId()), tgraph.getVertex(u.getId()), convertLabel("test"));

        assertEquals(tgraph.getVertex(v.getId()), v);
        assertEquals(tgraph.getVertex(u.getId()), u);
        if (tgraph.getFeatures().supportsEdgeRetrieval)
            assertEquals(tgraph.getEdge(e.getId()), e);

        vertexCount(tgraph, 2);
        edgeCount(tgraph, 1);

        tgraph.rollback();
        printPerformance(tgraph.toString(), 1, "edge not added in failed transaction (w/ iteration)");

        assertEquals(tgraph.getVertex(v.getId()), v);
        assertEquals(tgraph.getVertex(u.getId()), u);
        if (tgraph.getFeatures().supportsEdgeRetrieval)
            assertNull(tgraph.getEdge(e.getId()));

        if (tgraph.getFeatures().supportsVertexIteration)
            assertEquals(size(tgraph.getVertices()), 2);
        if (tgraph.getFeatures().supportsEdgeIteration)
            assertEquals(size(tgraph.getEdges()), 0);

        resetAndStart();

        e = tgraph.addEdge(null, tgraph.getVertex(u.getId()), tgraph.getVertex(v.getId()), convertLabel("test"));

        assertEquals(tgraph.getVertex(v.getId()), v);
        assertEquals(tgraph.getVertex(u.getId()), u);
        if (tgraph.getFeatures().supportsEdgeRetrieval)
            assertEquals(tgraph.getEdge(e.getId()), e);

        if (tgraph.getFeatures().supportsVertexIteration)
            assertEquals(size(tgraph.getVertices()), 2);
        if (tgraph.getFeatures().supportsEdgeIteration)
            assertEquals(size(tgraph.getEdges()), 1);
        assertEquals(e, getOnlyElement(tgraph.getVertex(u.getId()).getEdges(Direction.OUT)));
        tgraph.commit();
        printPerformance(tgraph.toString(), 1, "edge added in successful transaction (w/ iteration)");

        if (tgraph.getFeatures().supportsVertexIteration)
            assertEquals(size(tgraph.getVertices()), 2);
        if (tgraph.getFeatures().supportsEdgeIteration)
            assertEquals(size(tgraph.getEdges()), 1);

        assertEquals(tgraph.getVertex(v.getId()), v);
        assertEquals(tgraph.getVertex(u.getId()), u);
        if (tgraph.getFeatures().supportsEdgeRetrieval)
            assertEquals(tgraph.getEdge(e.getId()), e);
        assertEquals(e, getOnlyElement(tgraph.getVertex(u.getId()).getEdges(Direction.OUT)));
    }

    @Test
    public void testBruteEdgeTransactions() {
        TransactionalGraph tgraph = (TransactionalGraph) graph;
        resetAndStart();
        for (int i = 0; i < 100; i++) {
            Vertex v = tgraph.addVertex(null);
            Vertex u = tgraph.addVertex(null);
            tgraph.addEdge(null, v, u, convertLabel("test"));
            tgraph.commit();
        }
        printPerformance(tgraph.toString(), 100, "edges added in 100 successful transactions (2 vertices added for each edge)");
        vertexCount(tgraph, 200);
        edgeCount(tgraph, 100);

        resetAndStart();
        for (int i = 0; i < 100; i++) {
            Vertex v = tgraph.addVertex(null);
            Vertex u = tgraph.addVertex(null);
            tgraph.addEdge(null, v, u, convertLabel("test"));
            tgraph.rollback();
        }
        printPerformance(tgraph.toString(), 100, "edges not added in 100 failed transactions (2 vertices added for each edge)");
        vertexCount(tgraph, 200);
        edgeCount(tgraph, 100);

        resetAndStart();
        for (int i = 0; i < 100; i++) {
            Vertex v = tgraph.addVertex(null);
            Vertex u = tgraph.addVertex(null);
            tgraph.addEdge(null, v, u, convertLabel("test"));
        }
        vertexCount(tgraph, 400);
        edgeCount(tgraph, 200);
        tgraph.commit();
        printPerformance(tgraph.toString(), 100, "edges added in 1 successful transactions (2 vertices added for each edge)");
        vertexCount(tgraph, 400);
        edgeCount(tgraph, 200);

        resetAndStart();
        for (int i = 0; i < 100; i++) {
            Vertex v = tgraph.addVertex(null);
            Vertex u = tgraph.addVertex(null);
            tgraph.addEdge(null, v, u, convertLabel("test"));
        }
        vertexCount(tgraph, 600);
        edgeCount(tgraph, 300);

        tgraph.rollback();
        printPerformance(tgraph.toString(), 100, "edges not added in 1 failed transactions (2 vertices added for each edge)");
        vertexCount(tgraph, 400);
        edgeCount(tgraph, 200);
    }

    @Test
    public void testPropertyTransactions() {
        TransactionalGraph tgraph = (TransactionalGraph) graph;
        if (tgraph.getFeatures().supportsElementProperties()) {
            resetAndStart();
            Vertex v = tgraph.addVertex(null);
            Object id = v.getId();
            v.setProperty("name", "marko");
            tgraph.commit();
            printPerformance(tgraph.toString(), 1, "vertex added with string property in a successful transaction");


            resetAndStart();
            v = tgraph.getVertex(id);
            assertNotNull(v);
            assertEquals(v.getProperty("name"), "marko");
            v.setProperty("age", 30);
            assertEquals(v.getProperty("age"), 30);
            tgraph.rollback();
            printPerformance(tgraph.toString(), 1, "integer property not added in a failed transaction");

            resetAndStart();
            v = tgraph.getVertex(id);
            assertNotNull(v);
            assertEquals(v.getProperty("name"), "marko");
            assertNull(v.getProperty("age"));
            printPerformance(tgraph.toString(), 2, "vertex properties checked in a successful transaction");

            Edge edge = tgraph.addEdge(null, v, tgraph.addVertex(null), "test");
            edgeCount(tgraph, 1);
            tgraph.commit();
            edgeCount(tgraph, 1);
            edge = getOnlyElement(tgraph.getVertex(v.getId()).getEdges(Direction.OUT));
            assertNotNull(edge);

            resetAndStart();
            edge.setProperty("transaction-1", "success");
            assertEquals(edge.getProperty("transaction-1"), "success");
            tgraph.commit();
            printPerformance(tgraph.toString(), 1, "edge property added and checked in a successful transaction");
            edge = getOnlyElement(tgraph.getVertex(v.getId()).getEdges(Direction.OUT));
            assertEquals(edge.getProperty("transaction-1"), "success");

            resetAndStart();
            edge.setProperty("transaction-2", "failure");
            assertEquals(edge.getProperty("transaction-1"), "success");
            assertEquals(edge.getProperty("transaction-2"), "failure");
            tgraph.rollback();
            printPerformance(tgraph.toString(), 1, "edge property added and checked in a failed transaction");
            edge = getOnlyElement(tgraph.getVertex(v.getId()).getEdges(Direction.OUT));
            assertEquals(edge.getProperty("transaction-1"), "success");
            assertNull(edge.getProperty("transaction-2"));
        }
    }

    @Test
    public void testIndexTransactions() {
        TransactionalGraph tgraph = (TransactionalGraph) graph;
        if (tgraph.getFeatures().supportsVertexIndex) {
            resetAndStart();
            Index<Vertex> index = ((IndexableGraph) tgraph).createIndex("txIdx", Vertex.class);
            Vertex v = tgraph.addVertex(null);
            Object id = v.getId();
            v.setProperty("name", "marko");
            index.put("name", "marko", v);
            vertexCount(tgraph, 1);
            v = getOnlyElement(((IndexableGraph) tgraph).getIndex("txIdx", Vertex.class).get("name", "marko"));
            assertEquals(v.getId(), id);
            assertEquals(v.getProperty("name"), "marko");
            tgraph.commit();
            printPerformance(tgraph.toString(), 1, "vertex added and retrieved from index in a successful transaction");


            resetAndStart();
            vertexCount(tgraph, 1);
            v = getOnlyElement(((IndexableGraph) tgraph).getIndex("txIdx", Vertex.class).get("name", "marko"));
            assertEquals(v.getId(), id);
            assertEquals(v.getProperty("name"), "marko");
            printPerformance(tgraph.toString(), 1, "vertex retrieved from index outside successful transaction");


            resetAndStart();
            v = tgraph.addVertex(null);
            v.setProperty("name", "pavel");
            index.put("name", "pavel", v);
            vertexCount(tgraph, 2);
            v = getOnlyElement(((IndexableGraph) tgraph).getIndex("txIdx", Vertex.class).get("name", "marko"));
            assertEquals(v.getProperty("name"), "marko");
            v = getOnlyElement(((IndexableGraph) tgraph).getIndex("txIdx", Vertex.class).get("name", "pavel"));
            assertEquals(v.getProperty("name"), "pavel");
            tgraph.rollback();
            printPerformance(tgraph.toString(), 1, "vertex not added in a failed transaction");

            resetAndStart();
            vertexCount(tgraph, 1);
            assertEquals(size(((IndexableGraph) tgraph).getIndex("txIdx", Vertex.class).get("name", "pavel")), 0);
            printPerformance(tgraph.toString(), 1, "vertex not retrieved in a successful transaction");
            v = getOnlyElement(((IndexableGraph) tgraph).getIndex("txIdx", Vertex.class).get("name", "marko"));
            assertEquals(v.getProperty("name"), "marko");
        }
    }

    // public void testAutomaticIndexKeysRollback()

    @Test
    public void testAutomaticSuccessfulTransactionOnShutdown() throws IOException {

        TransactionalGraph tgraph = (TransactionalGraph) graph;
        if (tgraph.getFeatures().isPersistent && tgraph.getFeatures().supportsVertexProperties) {
            Vertex v = tgraph.addVertex(null);
            Object id = v.getId();
            v.setProperty("count", "1");
            v.setProperty("count", "2");
            tgraph.shutdown();
            tgraph = (TransactionalGraph) generateGraph();
            Vertex reloadedV = tgraph.getVertex(id);
            assertEquals("2", reloadedV.getProperty("count"));
        }
    }

    @Test
    public void testVertexCountOnPreTransactionCommit() {
        TransactionalGraph tgraph = (TransactionalGraph) graph;
        Vertex v1 = tgraph.addVertex(null);
        tgraph.commit();

        vertexCount(tgraph, 1);

        Vertex v2 = tgraph.addVertex(null);
        v1 = tgraph.getVertex(v1.getId());
        tgraph.addEdge(null, v1, v2, convertLabel("friend"));

        vertexCount(tgraph, 2);

        tgraph.commit();

        vertexCount(tgraph, 2);
    }

    @Test
    public void testVertexPropertiesOnPreTransactionCommit() {
        TransactionalGraph tgraph = (TransactionalGraph) graph;
        if (tgraph.getFeatures().supportsVertexProperties) {
            Vertex v1 = tgraph.addVertex(null);
            v1.setProperty("name", "marko");

            assertEquals(1, v1.getPropertyKeys().size());
            assertTrue(v1.getPropertyKeys().contains("name"));
            assertEquals("marko", v1.getProperty("name"));

            tgraph.commit();

            assertEquals("marko", v1.getProperty("name"));
        }
    }

    @Test
    public void testBulkTransactionsOnEdges() {
        TransactionalGraph tgraph = (TransactionalGraph) graph;
        for (int i = 0; i < 5; i++) {
            tgraph.addEdge(null, tgraph.addVertex(null), tgraph.addVertex(null), convertLabel("test"));
        }
        edgeCount(tgraph, 5);
        tgraph.rollback();
        edgeCount(tgraph, 0);

        for (int i = 0; i < 4; i++) {
            tgraph.addEdge(null, tgraph.addVertex(null), tgraph.addVertex(null), convertLabel("test"));
        }
        edgeCount(tgraph, 4);
        tgraph.rollback();
        edgeCount(tgraph, 0);


        for (int i = 0; i < 3; i++) {
            tgraph.addEdge(null, tgraph.addVertex(null), tgraph.addVertex(null), convertLabel("test"));
        }
        edgeCount(tgraph, 3);
        tgraph.commit();
        edgeCount(tgraph, 3);
    }


    @Test
    public void testCompetingThreads() {
        final TransactionalGraph tgraph = (TransactionalGraph) graph;
        int totalThreads = 250;
        final AtomicInteger vertices = new AtomicInteger(0);
        final AtomicInteger edges = new AtomicInteger(0);
        final AtomicInteger completedThreads = new AtomicInteger(0);
        for (int i = 0; i < totalThreads; i++) {
            new Thread() {
                public void run() {
                    Random random = new Random();
                    if (random.nextBoolean()) {
                        Vertex a = tgraph.addVertex(null);
                        Vertex b = tgraph.addVertex(null);
                        Edge e = tgraph.addEdge(null, a, b, convertLabel("friend"));

                        if (tgraph.getFeatures().supportsElementProperties()) {
                            a.setProperty("test", this.getId());
                            b.setProperty("blah", random.nextFloat());
                            e.setProperty("bloop", random.nextInt());
                        }
                        vertices.getAndAdd(2);
                        edges.getAndAdd(1);
                        tgraph.commit();
                    } else {
                        Vertex a = tgraph.addVertex(null);
                        Vertex b = tgraph.addVertex(null);
                        Edge e = tgraph.addEdge(null, a, b, convertLabel("friend"));
                        if (tgraph.getFeatures().supportsElementProperties()) {
                            a.setProperty("test", this.getId());
                            b.setProperty("blah", random.nextFloat());
                            e.setProperty("bloop", random.nextInt());
                        }
                        if (random.nextBoolean()) {
                            tgraph.commit();
                            vertices.getAndAdd(2);
                            edges.getAndAdd(1);
                        } else {
                            tgraph.rollback();
                        }
                    }
                    completedThreads.getAndAdd(1);
                }
            }.start();
        }

        while (completedThreads.get() < totalThreads) {
        }
        assertEquals(completedThreads.get(), 250);
        edgeCount(tgraph, edges.get());
        vertexCount(tgraph, vertices.get());
    }

//    @Test
//    public void testCompetingThreadsOnMultipleDbInstances() throws Exception {
        // the idea behind this test is to simulate a rexster environment where two graphs of the same type
        // are being mutated by multiple threads.  the test itself surfaced issues with OrientDB in such
        // an environment and remains relevant for any graph that might be exposed through rexster.

//        graphTest.dropGraph("first");
//        graphTest.dropGraph("second");
//
//        final TransactionalGraph graph1 = (TransactionalGraph) graphTest.generateGraph("first");
//        final TransactionalGraph graph2 = (TransactionalGraph) graphTest.generateGraph("second");
//
//
//        final Thread threadModFirstGraph = new Thread() {
//            public void run() {
//                final Vertex v = graph1.addVertex(null);
                // v.setProperty("name", "stephen");
//                graph1.commit();
//            }
//        };
//
//        threadModFirstGraph.start();
//        threadModFirstGraph.join();
//
//        final Thread threadReadBothGraphs = new Thread() {
//            public void run() {
//                int counter = 0;
//                for (Vertex v : graph1.getVertices()) {
//                    counter++;
//                }
//
//                assertEquals(1, counter);
//
//                counter = 0;
//                for (Vertex v : graph2.getVertices()) {
//                    counter++;
//                }
//
//                assertEquals(0, counter);
//            }
//        };
//
//        threadReadBothGraphs.start();
//        threadReadBothGraphs.join();
//
//
//        graph1.shutdown();
//        graphTest.dropGraph("first");
//
//        graph2.shutdown();
//        graphTest.dropGraph("second");
//    }

    @Test
    public void testTransactionIsolationCommitCheck() throws Exception {
        // the purpose of this test is to simulate rexster access to a graph instance, where one thread modifies
        // the graph and a separate thread cannot affect the transaction of the first
        final TransactionalGraph tgraph = (TransactionalGraph) graph;

        final CountDownLatch latchCommittedInOtherThread = new CountDownLatch(1);
        final CountDownLatch latchCommitInOtherThread = new CountDownLatch(1);

        // this thread starts a transaction then waits while the second thread tries to commit it.
        final Thread threadTxStarter = new Thread() {
            public void run() {
                final Vertex v = tgraph.addVertex(null);

                // System.out.println("added vertex");

                latchCommitInOtherThread.countDown();

                try {
                    latchCommittedInOtherThread.await();
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }

                tgraph.rollback();

                // there should be no vertices here
                // System.out.println("reading vertex before tx");
                assertFalse(tgraph.getVertices().iterator().hasNext());
                // System.out.println("read vertex before tx");
            }
        };

        threadTxStarter.start();

        // this thread tries to commit the transaction started in the first thread above.
        final Thread threadTryCommitTx = new Thread() {
            public void run() {
                try {
                    latchCommitInOtherThread.await();
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }

                // try to commit the other transaction
                tgraph.commit();

                latchCommittedInOtherThread.countDown();
            }
        };

        threadTryCommitTx.start();

        threadTxStarter.join();
        threadTryCommitTx.join();
    }

    @Test
    public void testRemoveInTransaction() {
        TransactionalGraph tgraph = (TransactionalGraph) graph;
        edgeCount(tgraph, 0);

        Vertex v1 = tgraph.addVertex(null);
        Object v1id = v1.getId();
        Vertex v2 = tgraph.addVertex(null);
        tgraph.addEdge(null, v1, v2, convertLabel("test-edge"));
        tgraph.commit();

        edgeCount(tgraph, 1);
        Edge e1 = getOnlyElement(tgraph.getVertex(v1id).getEdges(Direction.OUT));
        assertNotNull(e1);
        tgraph.removeEdge(e1);
        edgeCount(tgraph, 0);
        Vertex v1a = tgraph.getVertex(v1id);
        log.info("id {}", v1id);
        log.info("v1 {}", v1a);
        assertNull(getOnlyElement(tgraph.getVertex(v1id).getEdges(Direction.OUT), null));
        tgraph.rollback();

        edgeCount(tgraph, 1);
        e1 = getOnlyElement(tgraph.getVertex(v1id).getEdges(Direction.OUT));
        assertNotNull(e1);

        tgraph.removeEdge(e1);
        tgraph.commit();

        edgeCount(tgraph, 0);
        assertNull(getOnlyElement(tgraph.getVertex(v1id).getEdges(Direction.OUT), null));
    }

    @Test
    public void untestSimulateRexsterIntegrationTests() throws Exception {
        // this test simulates the flow of rexster integration test.  integration tests requests are generally not made
        // in parallel, but it is expected each request they may be processed by different threads from a thread pool
        // for each request.  this test fails for orientdb given it's optimnisitc locking strategy.
        final TransactionalGraph tgraph = (TransactionalGraph) graph;
        if (tgraph.getFeatures().supportsKeyIndices) {
            final String id = "_ID";
            ((KeyIndexableGraph) tgraph).createKeyIndex(id, Vertex.class);

            final int numberOfVerticesToCreate = 100;
            final Random rand = new Random(12356);
            final List<String> graphAssignedIds = new ArrayList<String>();

            final ExecutorService executorService = Executors.newFixedThreadPool(4);

            for (int ix = 0; ix < numberOfVerticesToCreate; ix++) {
                final int id1 = ix;
                final int id2 = ix + numberOfVerticesToCreate + rand.nextInt();

                // add a vertex and block for the thread to complete
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        final Vertex v = tgraph.addVertex(null);
                        v.setProperty(id, id1);
                        tgraph.commit();

                        graphAssignedIds.add(v.getId().toString());
                    }
                }).get();

                if (ix > 0) {
                    // add a vertex and block for the thread to complete
                    executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            final Vertex v = tgraph.addVertex(null);
                            v.setProperty(id, id2);
                            tgraph.commit();

                            graphAssignedIds.add(v.getId().toString());
                        }
                    }).get();

                    // add an edge to two randomly selected vertices and block for the thread to complete. integration
                    // tests tend to fail here, so the code is replicated pretty closely to what is in rexster
                    // (i.e. serialization to JSON) even though that may have nothing to do with failures.
                    executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            final Vertex vActual1 = tgraph.getVertex(graphAssignedIds.get(rand.nextInt(graphAssignedIds.size())));
                            final Vertex vActual2 = tgraph.getVertex(graphAssignedIds.get(rand.nextInt(graphAssignedIds.size())));
                            final Edge e = tgraph.addEdge(null, vActual1, vActual2, "knows");
                            e.setProperty("weight", rand.nextFloat());

                            JSONObject elementJson = null;
                            try {
                                // just replicating rexster
                                elementJson = GraphSONUtility.jsonFromElement(e, null, GraphSONMode.NORMAL);
                            } catch (Exception ex) {
                                fail();
                            }

                            tgraph.commit();

                            try {
                                if (elementJson != null) {
                                    // just replicating rexster
                                    elementJson.put("_ID", e.getId());
                                }
                            } catch (Exception ex) {
                                fail();
                            }
                        }
                    }).get();
                }
            }

            final Set<String> ids = new HashSet<String>();
            for (final Vertex v : tgraph.getVertices()) {
                ids.add(v.getId().toString());
            }

            for (final String idToRemove : ids) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        final Vertex toRemove = tgraph.getVertex(idToRemove);
                        tgraph.removeVertex(toRemove);

                        tgraph.commit();
                    }
                });
            }

            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void untestSimulateRexsterIntegrationTestsWithRetries() throws Exception {
        // this test simulates the flow of rexster integration test. integration tests requests are generally not made
        // in parallel, but it is expected each request they may be processed by different threads from a thread pool
        // for each request...this test is similar to the previous one but includes retries.  in this case,
        // orientdb passes, but this isn't currently how Rexster integration tests work.
        final TransactionalGraph tgraph = (TransactionalGraph) graph;
        if (tgraph.getFeatures().supportsKeyIndices) {
            final String id = "_ID";
            ((KeyIndexableGraph) tgraph).createKeyIndex(id, Vertex.class);

            final int maxRetries = 10;
            final int numberOfVerticesToCreate = 100;
            final Random rand = new Random(12356);
            final List<String> graphAssignedIds = new ArrayList<String>();

            final ExecutorService executorService = Executors.newFixedThreadPool(4);

            for (int ix = 0; ix < numberOfVerticesToCreate; ix++) {
                final int id1 = ix;
                final int id2 = ix + numberOfVerticesToCreate + rand.nextInt();

                // add a vertex and block for the thread to complete
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        final Vertex v = tgraph.addVertex(null);
                        v.setProperty(id, id1);
                        tgraph.commit();

                        graphAssignedIds.add(v.getId().toString());
                    }
                }).get();

                if (ix > 0) {
                    // add a vertex and block for the thread to complete
                    executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            final Vertex v = tgraph.addVertex(null);
                            v.setProperty(id, id2);
                            tgraph.commit();

                            graphAssignedIds.add(v.getId().toString());
                        }
                    }).get();

                    // add an edge to two randomly selected vertices and block for the thread to complete. integration
                    // tests tend to fail here, so the code is replicated pretty closely to what is in rexster
                    // (i.e. serialization to JSON) even though that may have nothing to do with failures.
                    executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            int v1 = rand.nextInt(graphAssignedIds.size());
                            int v2 = rand.nextInt(graphAssignedIds.size());

                            for (int retry = 0; retry < maxRetries; ++retry) {
                                try {
                                    final Vertex vActual1 = tgraph.getVertex(graphAssignedIds.get(v1));
                                    final Vertex vActual2 = tgraph.getVertex(graphAssignedIds.get(v2));
                                    final Edge e = tgraph.addEdge(null, vActual1, vActual2, "knows");
                                    e.setProperty("weight", rand.nextFloat());

                                    // just replicating rexster
                                    final JSONObject elementJson = GraphSONUtility.jsonFromElement(e, null, GraphSONMode.NORMAL);

                                    tgraph.commit();

                                    if (elementJson != null) {
                                        // just replicating rexster
                                        elementJson.put("_ID", e.getId());
                                    }
                                    break;
                                } catch (Exception ex) {
                                    if (!ex.getClass().getSimpleName().equals("OConcurrentModificationException"))
                                        fail(ex.getMessage());
                                }
                            }
                        }
                    }).get();
                }
            }

            final Set<String> ids = new HashSet<String>();
            for (final Vertex v : tgraph.getVertices()) {
                ids.add(v.getId().toString());
            }

            for (final String idToRemove : ids) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        final Vertex toRemove = tgraph.getVertex(idToRemove);
                        tgraph.removeVertex(toRemove);

                        tgraph.commit();
                    }
                });
            }

            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void untestTransactionVertexPropertiesAcrossThreads() throws Exception {
        // the purpose of this test is to ensure that properties of a element are available prior to commit()
        // across threads
        final TransactionalGraph tgraph = (TransactionalGraph) graph;
        if (graph.getFeatures().supportsThreadedTransactions) {

            final AtomicReference<Vertex> v = new AtomicReference<Vertex>();
            final Thread thread = new Thread() {
                public void run() {
                    final Vertex vertex = tgraph.addVertex(null);
                    vertex.setProperty("name", "stephen");
                    v.set(vertex);
                }
            };

            thread.start();
            thread.join();
            Set<String> k = v.get().getPropertyKeys();
            assertTrue(k.contains("name"));
            assertEquals("stephen", v.get().getProperty("name"));
        }
    }

    @Test
    public void untestTransactionIsolationWithSeparateThreads() throws Exception {
        // the purpose of this test is to simulate rexster access to a graph instance, where one thread modifies
        // the graph and a separate thread reads before the transaction is committed.  the expectation is that
        // the changes in the transaction are isolated to the thread that made the change and the second thread
        // should not see the change until commit() in the first thread.
        final TransactionalGraph tgraph = (TransactionalGraph) graph;

        final CountDownLatch latchCommit = new CountDownLatch(1);
        final CountDownLatch latchFirstRead = new CountDownLatch(1);
        final CountDownLatch latchSecondRead = new CountDownLatch(1);

        final Thread threadMod = new Thread() {
            public void run() {
                final Vertex v = tgraph.addVertex(null);
                //v.setProperty("name", "stephen");

                // System.out.println("added vertex");

                latchFirstRead.countDown();

                try {
                    latchCommit.await();
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }

                tgraph.commit();

                // System.out.println("committed vertex");

                latchSecondRead.countDown();
            }
        };

        threadMod.start();

        final Thread threadRead = new Thread() {
            public void run() {
                try {
                    latchFirstRead.await();
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }

                log.info("reading vertex before tx");
                assertFalse(tgraph.getVertices().iterator().hasNext());
                log.info("read vertex before tx");

                latchCommit.countDown();

                try {
                    latchSecondRead.await();
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }

                log.info("trying commit...");
//                gg.beginTx();
//                tgraph.commit();
//                log.info("reading vertex after {}", gg.repo().getHead());

                assertTrue(tgraph.getVertices().iterator().hasNext());
                log.info("read vertex after tx");
            }
        };

        threadRead.start();

        threadMod.join();
        threadRead.join();

    }
}