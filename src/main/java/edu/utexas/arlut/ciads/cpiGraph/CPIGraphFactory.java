package edu.utexas.arlut.ciads.cpiGraph;


import com.tinkerpop.blueprints.TransactionalGraph;
import edu.utexas.arlut.ciads.cacheGraph.IdFactory;
import lombok.Setter;

public class CPIGraphFactory {
    public static final String ID = "__id";

    CPIGraphFactory(TransactionalGraph writeThrough) {
        this.writeThrough = writeThrough;
    }
    CPIGraph get(String graphId) {
        return null;
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
