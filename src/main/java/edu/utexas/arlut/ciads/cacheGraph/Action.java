package edu.utexas.arlut.ciads.cacheGraph;

public interface Action {
    void apply(/* graph? */) /* throw? */;

    void reverse(/* graph */);

    public static class AddVertex implements Action {
        final CachedVertex.Impl cv;

        AddVertex(CachedVertex.Impl cv) {
            this.cv = cv;
        }

        @Override
        public void apply() {

        }

        @Override
        public void reverse() {

        }
    }

    public static class RemoveVertex implements Action {
        final CachedVertex.Impl cv;

        RemoveVertex(CachedVertex.Impl cv) {
            this.cv = cv;
        }

        @Override
        public void apply() {

        }

        @Override
        public void reverse() {

        }
    }
}
