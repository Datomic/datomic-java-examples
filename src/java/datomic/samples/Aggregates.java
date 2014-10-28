package datomic.samples;

import datomic.Connection;
import datomic.Database;

import java.io.IOException;

import static datomic.Peer.query;
import static datomic.samples.Fns.scratchConnection;
import static datomic.samples.IO.transactAllFromResource;
import static datomic.samples.PrettyPrint.print;

public class Aggregates {

    public static void main(String[] args) throws IOException {
        Connection conn = scratchConnection();
        transactAllFromResource(conn, "datomic-java-examples/bigger-than-pluto.edn");
        Database db = conn.db();

        print(biggestObjectRadius(db));
        print(randomObject(db));
        print(sampleFiveObjects(db));

    }

    public static Object biggestObjectRadius(Database db) {
        return query("[:find (max ?radius) ." +
                     " :where [_ :object/meanRadius ?radius]]", db);
    }

    public static Object randomObject(Database db) {
        return query("[:find (rand ?name) ." +
                     " :where [?e :object/name ?name]]", db);
    }

    public static Object sampleFiveObjects(Database db) {
        return query("[:find (sample 5 ?name) ." +
                     " :with ?e" +
                     " :where [?e :object/name ?name]]", db);
    }
}
