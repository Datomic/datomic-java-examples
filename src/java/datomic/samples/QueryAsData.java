package datomic.samples;

import datomic.Connection;
import datomic.Database;
import datomic.Peer;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URL;
import java.util.List;

import static datomic.Peer.q;
import static datomic.Util.*;
import static datomic.samples.IO.*;
import static datomic.samples.Fns.scratchConnection;

public class QueryAsData {
    public static Object tempid() {
        return Peer.tempid(":db.part/user");
    }
    public static void main(String[] args) throws IOException {
        Connection conn = scratchConnection();

        URL url = resource("datomic-java-examples/social-news.edn");

        transactAll(conn, new InputStreamReader(url.openStream()));

        // tx data is plain lists and maps
        conn.transact(
            list(map(":db/id", tempid(), ":user/firstName", "Stewart", ":user/lastName", "Brand"),
                 map(":db/id", tempid(), ":user/firstName", "Stuart", ":user/lastName", "Smalley"),
                 map(":db/id", tempid(), ":user/firstName", "John", ":user/lastName", "Stewart")));

        Database db = conn.db();

        // Find all Stewart first names (should return 1 tuple)
        System.out.println(q("[:find ?e :in $ ?name :where [?e :user/firstName ?name]]", db, "Stewart"));

        // Find all Stewart or Stuart first names (should return 2 tuples)
        List names = list("Stewart", "Stuart");
        System.out.println(q("[:find ?e :in $ [?name ...] :where [?e :user/firstName ?name]]", db, names));

        // Find all [Stewart|Stuart] [first|last] names (should return 3 tuples)
        List nameAttributes = list(read(":user/firstName"), read(":user/lastName"));
        System.out.println(q("[:find ?e :in $ [?name ...] [?attr ...] :where [?e ?attr ?name]]", db, names, nameAttributes));

        // Build a query out of data.  You might need this if e.g. writing a query optimizer.
        // Do *not* do this if parameterizing inputs (as shown above) is sufficient!
        List firstNameQuery = list(read(":find"), read("?e"),
                                   read(":in"), read("$"), read("?name"),
                                   read(":where"), list(read("?e"), read(":user/firstName"), read("?name")));

        // Find all Stewart first names (should return 1 tuple)
        System.out.println(q(firstNameQuery, db, "Stewart"));
    }
}
