/**
 * Datomic example code
 */

import datomic.Entity;
import datomic.Connection;
import datomic.Database;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map;

import static datomic.Peer.connect;
import static datomic.Peer.createDatabase;
import static datomic.Peer.q;

public class PrintSchema {
    /**
     *  Print an entity
     */
    public static void printEntity(Entity e) {
        Set keys = e.keySet();
        for (Iterator iterator = keys.iterator(); iterator.hasNext();) {
            Object key = iterator.next();
            System.out.println(key + " = " + e.get(key));
        }
    }

    /**
     * Print results from a query that returns entities in 1-tuples.
     * @param tuples
     */
    public static void printEntities(Collection<List<Object>> tuples) {
        for (Iterator<List<Object>> iterator = tuples.iterator(); iterator.hasNext();) {
            System.out.println();
            printEntity((Entity) iterator.next().get(0));
        }
    }

    /**
     * Schemas are plain data, like everything else. Values of
     * :db.install/attribute are the attributes defined in the schema.
     * @param db
     */
    public static void printAttributeSchema(Database db) {
        Collection<List<Object>> tuples = q("[:find ?entity" +
                                            " :where [_ :db.install/attribute ?v]" +
                                                    "[(.entity $ ?v) ?entity]]",
                                            db);
        printEntities(tuples);
    }

    public static void main(String[] args) {
        String uri = "datomic:mem://db";
        createDatabase(uri);
        Connection conn = connect(uri);

        String query = "[:find ?entity " +
                        ":in $ ?s " +
                        ":where [?e :db/valueType]" +
                               "[?e :db/ident ?a]" +
                               "[(namespace ?a) ?ns]" +
                               "[(= ?ns ?s)]" +
                               "[(.entity $ ?e) ?entity]]";
        Collection<List<Object>> results = q(query,conn.db(),"db");
        printEntities(results);
        // printAttributeSchema(conn.db());
    }
}
