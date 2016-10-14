package datomic.samples;


import datomic.Connection;
import datomic.Database;
import datomic.Datom;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static datomic.Util.list;
import static datomic.Connection.TX_DATA;
import static datomic.samples.Fns.scratchConnection;
import static datomic.samples.IO.transactAllFromResource;

public class ComponentAttributes {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        Connection conn = scratchConnection();
        transactAllFromResource(conn, "datomic-java-examples/social-news.edn");
        transactAllFromResource(conn, "datomic-java-examples/story-with-comments.edn");
        Database db = conn.db();

        System.out.println("\nTouch an entity with componeont attributes...");
        System.out.println(db.entity(":storyWithComments").touch());

        System.out.println("\nDoc string for :db.fn/retractEntity...");
        System.out.println(db.entity(":db.fn/retractEntity").touch());

        Collection<Datom> datoms =
        (Collection<Datom>) conn.transact(list(list(":db.fn/retractEntity", ":storyWithComments")))
                                .get()
                                .get(TX_DATA);
        Set retractedEs = new HashSet();
        for (Iterator<Datom> iterator = datoms.iterator(); iterator.hasNext(); ) {
            Datom datom = iterator.next();
            if (!datom.added()) retractedEs.add(datom.e());
        }

        System.out.println("\nRetracted entities " + retractedEs);
    }
}
