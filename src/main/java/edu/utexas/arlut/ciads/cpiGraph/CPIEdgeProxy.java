package edu.utexas.arlut.ciads.cpiGraph;

import static com.tinkerpop.blueprints.util.ExceptionFactory.bothIsNotSupported;

import com.google.common.base.Function;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StringFactory;

public class CPIEdgeProxy extends CPIElementProxy implements Edge {
    public static Function<String, CPIEdgeProxy> MAKE(final CPIGraph g) {
        return new Function<String, CPIEdgeProxy>() {
            @Override
            public CPIEdgeProxy apply(String id) {
                return new CPIEdgeProxy(id, g);
            }
        };
    }
    public static final Function<CPIEdgeProxy, Edge> DOWNCAST = new Function<CPIEdgeProxy, Edge>() {
        @Override
        public Edge apply(CPIEdgeProxy input) {
            return (Edge) input;
        }
    };
    protected CPIEdgeProxy(String id, CPIGraph g) {
        super(id, g);
    }
    @Override
    CPIEdge getImpl() {
        return graph.edgeImpl(id);
    }
    CPIEdge getMutableImpl() {
        return graph.mutableEdgeImpl(id);
    }
    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        if (direction.equals(Direction.OUT))
            return outVertex();
        if (direction.equals(Direction.IN))
            return inVertex();
        throw bothIsNotSupported();
    }
    CPIVertexProxy outVertex() {
        return new CPIVertexProxy(getImpl().outVertexId, graph);
    }
    CPIVertexProxy inVertex() {
        return new CPIVertexProxy(getImpl().inVertexId, graph);
    }

    @Override
    public String getLabel() {
        return getImpl().label;
    }
    @Override
    public void remove() {
        graph.removeEdge(this);
    }
    // =================================
    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
    // =================================
    public static class CPIEdge extends CPIElement {
        CPIEdge(String id, String outVertexId, String inVertexId, String label) {
            super(id);
            this.outVertexId = outVertexId;
            this.inVertexId = inVertexId;
            this.label = label;
        }
        CPIEdge(CPIEdge src) {
            super(src);
            this.outVertexId = src.outVertexId;
            this.inVertexId = src.inVertexId;
            this.label = src.label;
        }
        final String outVertexId, inVertexId;
        final String label;
    }
}
