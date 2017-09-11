package edu.utexas.arlut.ciads;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Map;

import static com.tinkerpop.blueprints.util.ElementHelper.getProperties;

@Slf4j
public class TestUtil {
    protected static void deleteDirectory(final File directory) {
        if (directory.exists()) {
            for (File file : directory.listFiles()) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
            directory.delete();
        }

        // overkill code, simply allowing us to detect when data dir is in use.  useful though because without it
        // tests may fail if a database is re-used in between tests somehow.  this directory really needs to be
        // cleared between tests runs and this exception will make it clear if it is not.
        if (directory.exists()) {
            throw new RuntimeException("unable to delete directory " + directory.getAbsolutePath());
        }
    }
    // =======================================
    private static void dumpProperties(Element e) {
        for (Map.Entry<String, Object> me : getProperties(e).entrySet())
            log.info("\t{} => {}", me.getKey(), me.getValue());
    }

    public static void dumpGraph(Graph g) {
        for (Vertex v : g.getVertices()) {
            log.info("v: {}", v);
            dumpProperties(v);
        }
        for (Edge e : g.getEdges()) {
            log.info("e: {}", e);
            dumpProperties(e);
        }
    }
}
