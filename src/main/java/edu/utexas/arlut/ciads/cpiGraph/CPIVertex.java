package edu.utexas.arlut.ciads.cpiGraph;

import com.google.common.base.Function;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

public class CPIVertex extends CPIElement implements Vertex {
    public static Function<String, CPIVertex> MAKE(final CPIGraph g) {
        return new Function<String, CPIVertex>() {
            @Override
            public CPIVertex apply(String id) {
                return new CPIVertex(id, g);
            }
        };
    }
    public static final Function<CPIVertex, Vertex> DOWNCAST = new Function<CPIVertex, Vertex>() {
        @Override
        public Vertex apply(CPIVertex input) {
            return (Vertex) input;
        }
    };

    protected CPIVertex(String id, CPIGraph g) {
        super(id, g);
    }
    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        return null;
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        return null;
    }

    @Override
    public VertexQuery query() {
        return null;
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex) {
        return null;
    }

    public static class Impl extends CPIElement.Impl {
        Impl(String id) {
            super(id);
        }
        Impl(Impl src) {
            super(src);
        }

    }
}
