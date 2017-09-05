package edu.utexas.arlut.ciads.cacheGraph;


public interface ImplContainer {
    CachedVertex.Impl getVertex(String id);
    CachedVertex.Impl getMutableVertex(String id);

//    CachedEdge.VertexImpl getVertex(String id);
//    CachedEdge.VertexImpl getMutableVertex(String id);
}
