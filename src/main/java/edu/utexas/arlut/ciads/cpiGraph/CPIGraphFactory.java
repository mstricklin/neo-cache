package edu.utexas.arlut.amt.graph.impl.cpiGraph;


import com.tinkerpop.blueprints.TransactionalGraph;
import lombok.Setter;

public class CPIGraphFactory {

    public CPIGraphFactory(TransactionalGraph writeThrough) {
        this.writeThrough = writeThrough;
        vertexIdFactory = new IdFactory.DefaultIdFactory();
        edgeIdFactory = new IdFactory.DefaultIdFactory();
    }
    public CPIGraph get(String graphId) {
        return new CPIGraph(graphId, this);
    }
    // =======================================
    String vertexId() {
        return vertexIdFactory.call();
    }

    String edgeId() {
        return edgeIdFactory.call();
    }

    // =======================================

    private final TransactionalGraph writeThrough;
    @Setter
    IdFactory vertexIdFactory;
    @Setter
    IdFactory edgeIdFactory;

}
