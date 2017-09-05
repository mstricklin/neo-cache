package edu.utexas.arlut.amt.graph.impl.cpiGraph;

import java.util.UUID;

public interface IdFactory {

    String ID = "__id";

    String call();

    class DefaultIdFactory implements IdFactory {
        @Override
        public String call() {
            return UUID.randomUUID().toString();
        }
    }
}
