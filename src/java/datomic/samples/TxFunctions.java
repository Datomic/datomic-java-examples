package datomic.samples;

import datomic.Connection;
import datomic.Database;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.lang.IllegalStateException;
import java.util.List;
import java.util.Map;

import static datomic.Peer.query;
import static datomic.Peer.tempid;
import static datomic.Peer.toT;
import static datomic.Peer.function;
import static datomic.Util.list;
import static datomic.samples.Fns.scratchConnection;
import static datomic.Util.map;
import static datomic.Util.read;

public class TxFunctions {

    /*
     * Function intended for use as a Datomic transaction function. ensureComposite will fail to transact if the
     * existing two key/value combinations specified in parameters are already set for an entity in the database.
     *
     * The resulting exception will report the entity and the tx value.
     *
     * @param db      a database value
     * @param k1      first key of composite uniqueness constraint
     * @param v1      value corresponding to first key
     * @param k2      second key
     * @param v2      second value
     * @return        transaction data to create entity with attr/value pairs k1:v1, k2:v2.
     */
    public static List ensureComposite(Database db, Object k1, Object v1, Object k2, Object v2) {
        String compositeQuery = "[:find [?e ?t1 ?t2] " +
                                 ":in $ ?k1 ?v1 ?k2 ?v2 " +
                                 ":where " +
                                 "[?e ?k1 ?v1 ?t1] " +
                                 "[?e ?k2 ?v2 ?t2]]";
        List<Object> conflict = query(compositeQuery, db, k1, v1, k2, v2);
        if (conflict != null && !conflict.isEmpty()) {
            Long entId = (Long) conflict.get(0);
            Long t1 = (Long) conflict.get(1);
            Long t2 = (Long) conflict.get(2);
            Long tMax = toT(Math.max(t1, t2));
            throw new IllegalStateException("Composite key exists: " + entId + " at t: " + tMax);
        } else {
            return list(map(read(":db/id"), tempid(read(":db.part/user")),
                            k1, v1, k2, v2));
        }
    }

    /*
     * Installs the ensureComposite function using a string literal representation of its source code.
     */
    public static Map ensureCompositeInstall(Connection conn) throws InterruptedException, ExecutionException {
        String source = "import java.util.List;\nimport static datomic.Peer.query;\nimport static datomic.Peer.toT;\n" +
                "import static datomic.Util.list;\nimport static datomic.Util.map;\nimport static datomic.Util.read;\n" +
               "String compositeQuery = \"[:find [?e ?t1 ?t2] \" +\n" +
               "            \":in $ ?k1 ?v1 ?k2 ?v2 \" +\n" +
               "            \":where \" +\n" +
               "            \"[?e ?k1 ?v1 ?t1] \" +\n" +
               "            \"[?e ?k2 ?v2 ?t2]]\";\n" +
               "    List<Object> conflict = query(compositeQuery, db, k1, v1, k2, v2);\n" +
               "    if (conflict != null && !conflict.isEmpty()) {\n" +
               "        Long entId = (Long) conflict.get(0);\n" +
               "        Long t1 = (Long) conflict.get(1);\n" +
               "        Long t2 = (Long) conflict.get(2);\n" +
               "        Long tMax = toT(Math.max(t1, t2));\n" +
               "        throw new IllegalStateException(\"Composite key exists: \" + entId + \" at t: \" + tMax);\n" +
               "    } else {\n" +
               "        return list(map(read(\":db/id\"), tempid(read(\":db.part/user\")),\n" +
               "                k1, v1, k2, v2));\n" +
               "    }\n";
        datomic.functions.Fn ensureComposite =
                                      function(map(read(":lang"), "java",
                                                   read(":params"), list(read("db"),read("k1"),read("v1"),read("k2"),read("v2")),
                                                   read(":code"), source));
        Future<Map> txResult =
                    conn.transact(list(map(read(":db/id"), tempid(read(":db.part/user")),
                                           read(":db/fn"), ensureComposite,
                                           read(":db/ident"), read(":examples/ensure-composite"),
                                           read(":db/doc"), "Create an entity with k1=v1, k2=v2, throwing if such an entity already exists")));
        return txResult.get();
    }


    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        // get a connection to a new scratch db and install transactor function
        Connection conn = scratchConnection();
        Map installResult = ensureCompositeInstall(conn);
        // work with database after txFunction has been installed
        Database dbAfter = (Database) installResult.get(read(":db-after"));

        // invoke locally to test
        dbAfter.invoke(read(":examples/ensure-composite"), dbAfter, read(":db/ident"), read(":examples/test"), read(":db/doc"), "This one works");
        // commented line below will throw (key/value pairs exist)
        // dbAfter.invoke(read(":examples/ensure-composite"), dbAfter, read(":db/ident"), read(":db/txInstant"), read(":db/index"), read("true"));

        // now invoke in a transaction function
        Future<Map> fnInvokeResultConflict =
                conn.transact(list(list(read(":examples/ensure-composite"), read(":db/ident"), read(":db/txInstant"), read(":db/index"), read("true"))));
        Future<Map> fnInvokeResultOK =
                conn.transact(list(list(read(":examples/ensure-composite"), read(":db/ident"),read(":examples/test"), read(":db/doc"), "No conflict.")));
        System.out.println(fnInvokeResultOK.get());
        try {
            fnInvokeResultConflict.get();
        } catch (ExecutionException e) {
            // transaction future throws an execution exception when we retrieve it, but IllegalStateException is
            // displayed in the resulting message with the error message we set in function.
            System.out.println("Transaction function failed with:\n" + e.getMessage());
        }
    }

}
