package edu.utexas.arlut.ciads.cpiGraph;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.Sets.newHashSet;
import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;
import static java.util.Arrays.asList;

import java.util.*;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;
import com.tinkerpop.blueprints.util.StringFactory;
import com.tinkerpop.blueprints.util.VerticesFromEdgesIterable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CPIVertexProxy extends CPIElementProxy implements Vertex {
    public static Function<String, CPIVertexProxy> MAKE(final CPIGraph g) {
        return new Function<String, CPIVertexProxy>() {
            @Override
            public CPIVertexProxy apply(String id) {
                return new CPIVertexProxy(id, g);
            }
        };
    }
    public static Function<CPIVertex, CPIVertexProxy> PROXY(final CPIGraph g) {
        return new Function<CPIVertex, CPIVertexProxy>() {
            @Override
            public CPIVertexProxy apply(CPIVertex v) {
                return new CPIVertexProxy(v.id, g);
            }
        };
    }
    public static final Function<CPIVertexProxy, Vertex> DOWNCAST = new Function<CPIVertexProxy, Vertex>() {
        @Override
        public Vertex apply(CPIVertexProxy input) {
            return (Vertex)input;
        }
    };

    protected CPIVertexProxy(String id, CPIGraph g) {
        super(id, g);
    }
    // =================================
    CPIVertex getImpl() {
        return graph.vertexImpl(id);
    }
    CPIVertex getMutableImpl() {
        return graph.mutableVertexImpl(id);
    }
    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        CPIVertex impl = graph.vertexImpl(id);
        if (OUT == direction) {
            return getEdges(impl.outEdges, asList(labels));
        } else if (IN == direction) {
            return getEdges(impl.inEdges, asList(labels));
        } else if (direction.equals((Direction.BOTH))) {
            Iterable<String> edges = Iterables.concat(impl.inEdges, impl.outEdges);
            return getEdges(edges, asList(labels));
        }
        return Collections.emptyList();

    }
    private Iterable<Edge> getEdges(Iterable<String> edgeIDs, final Collection<String> labels) {
        FluentIterable<CPIEdgeProxy> f = FluentIterable.from(edgeIDs).transform(CPIEdgeProxy.MAKE(graph));
        if (labels.isEmpty()) {
            return f.transform(CPIEdgeProxy.DOWNCAST).toList();
        }
        Predicate<CPIEdgeProxy> labelled = new Predicate<CPIEdgeProxy>() {
            @Override
            public boolean apply(CPIEdgeProxy e) {
                return labels.contains(e.getLabel());
            }
        };
        return f.filter(labelled).transform(CPIEdgeProxy.DOWNCAST).toList();
    }
    void removeEdge(String edgeId) {
        getMutableImpl().outEdges.remove(edgeId);
        getMutableImpl().inEdges.remove(edgeId);
    }
    // =================================
    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        return new VerticesFromEdgesIterable(this, direction, labels);
    }
    @Override
    protected void _setProperty(String key, Object value) {
        graph.persister.setVProperty(id, key, value);
    }
    @Override
    protected void _rmProperty(String key) {
        graph.persister.removeVProperty(id, key);
    }

    @Override
    public VertexQuery query() {
        return new DefaultVertexQuery(this);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex) {
        return graph.addEdge(null, this, inVertex, label);
    }
    void addOutEdge(String edgeId) {
        getMutableImpl().outEdges.add(edgeId);
    }
    void addInEdge(String edgeId) {
        getMutableImpl().inEdges.add(edgeId);
    }
    @Override
    public void remove() {
        graph.removeVertex(this);
    }
    // =================================
    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
    // =================================
    public static class CPIVertex extends CPIElement {
        CPIVertex(String id) {
            super(id);
            outEdges = newHashSet();
            inEdges = newHashSet();
        }
        CPIVertex(CPIVertex src) {
            super(src);
            outEdges = newHashSet(src.outEdges);
            inEdges = newHashSet(src.inEdges);
//            base = src.base;
        }
        CPIVertex(String id, Vertex v) {
            super(id, v);
            outEdges = newHashSet();
            inEdges = newHashSet();
//            base = v;
        }
        final Set<String> outEdges;
        final Set<String> inEdges;

        // this *may* be populated later by the write-behind queue. It's an optimization to keep from repeatedly
        // looking up the underlying element from the id.
//        @Setter @Getter
//        protected Vertex base = null;
    }
}
