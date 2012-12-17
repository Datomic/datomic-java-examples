package datomic.samples;

import datomic.Connection;
import datomic.Database;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collection;
import java.util.List;

import static datomic.Peer.q;
import static datomic.Util.list;
import static datomic.samples.Fns.printQueryResult;
import static datomic.samples.Fns.scratchConnection;
import static datomic.samples.IO.resource;
import static datomic.samples.IO.transactAll;
import static datomic.samples.IO.transactAllFromResource;

public class BuildingQueries {

    public static void queryWithVariableBinding(Database db) {
        Collection result = q("[:find ?e" +
                              " :in $ ?name" +
                              " :where [?e :user/firstName ?name]]",
                db, "Stewart");
        printQueryResult(result);
    }

    public static void queryWithCollectionBinding(Database db) {
        Collection result = q("[:find ?e" +
                              " :in $ [?name ...]" +
                              " :where [?e :user/firstName ?name]]",
                db, list("Stewart", "Stuart"));
        printQueryResult(result);
    }

    public static void queryWithMultipleCollectionBindings(Database db) {
        Collection result = q("[:find ?e" +
                              " :in $ [?name ...] [?attr ...]" +
                              " :where [?e ?attr ?name]]",
                db, list("Stuart", "Stewart"), list(":user/firstName", ":user/lastName"));
        printQueryResult(result);
    }

    public static void queryWithMapForm(Database db) {
        Collection result = q("{:find [?e]" +
                              " :in [$ ?fname ?lname]" +
                              " :where [[?e :user/firstName ?fname]" +
                              "         [?e :user/lastName ?lname]]}",
                db, "Stuart", "Smalley");
        printQueryResult(result);
    }

    public static void main(String[] args) throws IOException {
        Connection conn = scratchConnection();
        transactAllFromResource(conn, "datomic-java-examples/social-news.dtm");
        transactAllFromResource(conn, "datomic-java-examples/stuarts.dtm");
        Database db = conn.db();

        queryWithVariableBinding(db);
        queryWithCollectionBinding(db);
        queryWithMultipleCollectionBindings(db);
        queryWithMapForm(db);
    }
}
