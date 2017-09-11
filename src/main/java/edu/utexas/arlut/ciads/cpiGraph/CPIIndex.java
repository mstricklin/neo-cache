package edu.utexas.arlut.ciads.cpiGraph;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;
import static edu.utexas.arlut.ciads.cpiGraph.CPIElementProxy.CPIElement;

public class CPIIndex<T extends CPIElement> {

    Set<String> keys() {
        return indices.keySet();
    }

    boolean has(String key) {
        return indices.containsKey(key);
    }

    void addIndex(String key) {
        if (!indices.containsKey(key))
            indices.put(key, makeIndex());
    }

    void dropIndex(String key) {
        indices.remove(key);
    }

    void index(T t) {
        Set<String> pkeys = t.properties.keySet();
        Set<String> indexedKeys = Sets.union(pkeys, keys());
        for (String k : indexedKeys) {
            indices.get(k).put(t.properties.get(k), t);
        }
    }

    void index(String key, Object value, T t) {
        if (indices.containsKey(key))
            indices.get(key).put(value, t);
    }

    Collection<T> indexed(String key, Object value) {
        return indices.get(key).get(value);
    }

    private Multimap<Object, T> makeIndex() {
        ListMultimap<Object, T> l = ArrayListMultimap.create();
        return Multimaps.synchronizedListMultimap(l);
    }

    final Map<String, Multimap<Object, T>> indices = newHashMap();
}
