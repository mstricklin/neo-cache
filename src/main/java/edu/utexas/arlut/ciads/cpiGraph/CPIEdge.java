package edu.utexas.arlut.ciads.cpiGraph;

import com.google.common.base.Function;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class CPIEdge extends CPIElement implements Edge {
    public static Function<String, CPIEdge> MAKE(final CPIGraph g) {
        return new Function<String, CPIEdge>() {
            @Override
            public CPIEdge apply(String id) {
                return new CPIEdge(id, g);
            }
        };
    }
    public static final Function<CPIEdge, Edge> DOWNCAST = new Function<CPIEdge, Edge>() {
        @Override
        public Edge apply(CPIEdge input) {
            return (Edge) input;
        }
    };
    protected CPIEdge(String id, CPIGraph g) {
        super(id, g);
    }
    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        return null;
    }

    @Override
    public String getLabel() {
        // graph.getImpl(id).label;
        return null;
    }
    public static class Impl extends CPIElement.Impl {
        Impl(String id, String outVertexId, String inVertexId, String label) {
            super(id);
            this.outVertexId = outVertexId;
            this.inVertexId = inVertexId;
            this.label = label;
        }
        Impl(Impl src) {
            super(src);
            this.outVertexId = src.outVertexId;
            this.inVertexId = src.inVertexId;
            this.label = src.label;
        }
        final String outVertexId, inVertexId;
        final String label;
    }
}
