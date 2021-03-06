package edu.utexas.arlut.ciads.cpiGraph;

import java.util.UUID;

public interface IdFactory {

    String call();

    class DefaultIdFactory implements IdFactory {
        @Override
        public String call() {
            return UUID.randomUUID().toString();
        }
    }
}
