package edu.utexas.arlut.ciads.cpiGraph;

import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.TransactionalGraph;

public interface SoRBuilder<T extends TransactionalGraph & KeyIndexableGraph> {

    T build(String dir);
}
