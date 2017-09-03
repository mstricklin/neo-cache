package edu.utexas.arlut.ciads;

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

public class CacheVertex extends CacheElement implements Vertex {

    CacheVertex(String id, CacheGraph cg) {
        super(id, cg);
    }
    CacheVertex(Vertex from, CacheGraph cg) {
        super(from, cg);
    }

    public Vertex getBaseVertex() {
        return (Vertex) this.baseElement;
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        // TODO: filter on labels
        if (direction.equals(com.tinkerpop.blueprints.Direction.OUT))
            return ImmutableList.copyOf(outEdges);
        else if (direction.equals(com.tinkerpop.blueprints.Direction.IN))
            return ImmutableList.copyOf(inEdges);
        else
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

    public void addOutEdge(CacheEdge ce) {
        outEdges.add(ce);
    }

    public void addInEdge(CacheEdge ce) {
        inEdges.add(ce);
    }

    @Override
    public void remove() {
        graph.removeVertex(this);
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    private Set<CacheEdge> outEdges = newHashSet();
    private Set<CacheEdge> inEdges = newHashSet();
}
