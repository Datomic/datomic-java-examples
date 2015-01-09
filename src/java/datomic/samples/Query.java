package datomic.samples;

import datomic.Connection;
import datomic.Database;
import datomic.Peer;

import java.util.Scanner;
import java.util.Collection;

import static datomic.Util.read;
import static datomic.samples.Fns.scratchConnection;
import static datomic.samples.Schema.CARDINALITY_MANY;
import static datomic.samples.Schema.CARDINALITY_ONE;
import static datomic.samples.Schema.cardinality;

public class Query {

    public static void main(String[] args) {

        // Your connection information for local mbrainz 1968-1973 subset goes here.
        String uri = "datomic:free://localhost:4334/mbrainz-1968-1973";

        // Connect to transactor, get latest database value from storage.
        Connection conn = Peer.connect(uri);
        Database db = conn.db();

        System.out.println("All releases in the database with release names.");
        Collection results = Peer.query("[:find ?release-name :where [_ :release/name ?release-name]]", db);
        System.out.println(results);
        pause();

        System.out.println("Total number of artists without :artist/country attribute.");
        Integer res = Peer.query("[:find (count ?eid) . " +
                                  ":where [?eid :artist/name] " +
                                         "(not [?eid :artist/country])]",
                                 db);
        System.out.println(res);
        pause();

        System.out.println("Number of artists missing either country or gender. "+
                           "(negation of conjunction reads 'not (clause1 and clause2)'");
        res = Peer.query("[:find (count ?eid) . " +
                          ":where [?eid :artist/name] " +
                                 "(not [?eid :artist/country] " +
                                      "[?eid :artist/gender])]",
                         db);
        System.out.println(res);
        pause();

    }

    private static final Scanner scanner = new Scanner(System.in);

    private static void pause() {
        if (System.getProperty("NOPAUSE") == null) {
            System.out.println("\nPress enter to continue...");
            scanner.nextLine();
        }
    }
}
