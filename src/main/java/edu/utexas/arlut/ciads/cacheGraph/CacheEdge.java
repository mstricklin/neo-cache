package edu.utexas.arlut.ciads.cacheGraph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

public class CacheEdge extends CacheElement implements Edge {

    protected CacheEdge(String id, CacheVertex outVertex, CacheVertex inVertex, String label, CacheGraph cg) {
        super(id, cg);
        startVertex = outVertex;
        endVertex = inVertex;
        this.label = label;
    }
    CacheEdge(Edge from, CacheVertex outVertex, CacheVertex inVertex, CacheGraph cg) {
        super(from, cg);
        startVertex = outVertex;
        endVertex = inVertex;
        label = from.getLabel();
    }

    public Edge getBaseEdge() {
        return (Edge) this.baseElement;
    }

    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        if (OUT == direction)
            return startVertex;
        else if (IN == direction)
            return endVertex;
        else
            throw ExceptionFactory.bothIsNotSupported();
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public void remove() {
        graph.removeEdge(this);
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }

    final CacheVertex startVertex, endVertex;
    final String label;
}
