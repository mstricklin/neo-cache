package edu.utexas.arlut.ciads.cpiGraph;

import static com.tinkerpop.blueprints.util.ExceptionFactory.bothIsNotSupported;

import com.google.common.base.Function;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StringFactory;
import lombok.Getter;
import lombok.Setter;

public class CPIEdgeProxy extends CPIElementProxy implements Edge {
    public static Function<String, CPIEdgeProxy> MAKE(final CPIGraph g) {
        return new Function<String, CPIEdgeProxy>() {
            @Override
            public CPIEdgeProxy apply(String id) {
                return new CPIEdgeProxy(id, g);
            }
        };
    }
    public static Function<CPIEdge, CPIEdgeProxy> PROXY(final CPIGraph g) {
        return new Function<CPIEdge, CPIEdgeProxy>() {
            @Override
            public CPIEdgeProxy apply(CPIEdge v) {
                return new CPIEdgeProxy(v.id, g);
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

    protected void _setProperty(String key, Object value) {
        graph.persister.setEProperty(id, key, value);
    }
    @Override
    protected void _rmProperty(String key) { graph.persister.removeEProperty(id, key);
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
//            this.base = src.base;
        }
        CPIEdge(String id, Edge e) {
            super(id, e);
            this.outVertexId = e.getVertex(Direction.OUT).getId().toString();
            this.inVertexId = e.getVertex(Direction.IN).getId().toString();
            this.label = e.getLabel();
//            base = e;
        }
        final String outVertexId, inVertexId;
        final String label;

        // this *may* be populated later by the write-behind queue. It's an optimization to keep from repeatedly
        // looking up the underlying element from the id.
//        @Setter @Getter
//        protected Edge base = null;
    }
}
