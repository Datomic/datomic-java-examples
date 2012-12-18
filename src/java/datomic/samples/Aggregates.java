package datomic.samples;

import datomic.Connection;
import datomic.Database;

import java.io.IOException;
import java.util.Collection;

import static datomic.Peer.q;
import static datomic.samples.Fns.printQueryResult;
import static datomic.samples.Fns.scratchConnection;
import static datomic.samples.IO.transactAllFromResource;

public class Aggregates {

    public static void main(String[] args) throws IOException {
        Connection conn = scratchConnection();
        transactAllFromResource(conn, "datomic-java-examples/bigger-than-pluto.edn");
        Database db = conn.db();

        printQueryResult(biggestObjectRadius(db));
        printQueryResult(randomObject(db));
        printQueryResult(sampleFiveObjects(db));

    }

    public static Collection biggestObjectRadius(Database db) {
        return q("[:find (max ?radius)" +
                "  :where [_ :object/meanRadius ?radius]]", db);
    }

    public static Collection randomObject(Database db) {
        return q("[:find (rand ?name)" +
                "  :where [?e :object/name ?name]]", db);
    }

    public static Collection sampleFiveObjects(Database db) {
        return q("[:find (sample 5 ?name)" +
                "  :with ?e" +
                "  :where [?e :object/name ?name]]", db);
    }
}
