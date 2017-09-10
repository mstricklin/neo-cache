package edu.utexas.arlut.ciads.cacheGraph;

import com.google.common.collect.*;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;
import com.tinkerpop.blueprints.util.StringFactory;
import com.tinkerpop.blueprints.util.VerticesFromEdgesIterable;

import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;

public class CachedVertex extends CachedElement implements Vertex {

    CachedVertex(String id, CachedGraph cg) {
        super(id, cg);
    }

    CachedVertex(Vertex from, CachedGraph cg) {
        super(from, cg);
    }

    public Vertex getBaseVertex() {
        return (Vertex) this.baseElement;
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        // TODO: filter on labels
//        if (direction.equals(com.tinkerpop.blueprints.Direction.OUT))
//            return ImmutableList.copyOf(outEdges);
//        else if (direction.equals(com.tinkerpop.blueprints.Direction.IN))
//            return ImmutableList.copyOf(inEdges);
//        else
            return new ImmutableList.Builder<Edge>()
                    .addAll(outEdges)
                    .addAll(inEdges)
                    .build();
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        return new VerticesFromEdgesIterable(this, direction, labels);
    }

    @Override
    public VertexQuery query() {
        return new DefaultVertexQuery(this);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex) {
        return graph.addEdge(null, this, inVertex, label);
    }

    public void addOutEdge(CachedEdge ce) {
        outEdges.add(ce);
    }

    public void addInEdge(CachedEdge ce) {
        inEdges.add(ce);
    }


    @Override
    public void setProperty(String key, Object value) {
        super.setProperty(key, value);
        graph.addToIndex(key, value, this);
    }

    @Override
    public <T> T removeProperty(String key) {
        Object value = getProperty(key);
        graph.removeFromIndex(key, value, this);
        return super.removeProperty(key);
    }

    @Override
    public void remove() {
        graph.removeVertex(this);
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    static class Impl extends CachedElement.Impl {
        Impl copy() {
            return new Impl(this);
        }
        Impl() {
            super();
            outEdges = newHashSet();
            inEdges = newHashSet();
        }
        Impl(Impl src) {
            super(src);
            outEdges = newHashSet(src.outEdges);
            inEdges = newHashSet(src.inEdges);
        }
        private Set<CachedEdge> outEdges = newHashSet();
        private Set<CachedEdge> inEdges = newHashSet();
    }

    private Set<CachedEdge> outEdges = newHashSet();
    private Set<CachedEdge> inEdges = newHashSet();
}
