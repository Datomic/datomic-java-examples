package datomic.samples;

import datomic.Connection;
import datomic.Database;
import datomic.Datom;

import javax.sql.rowset.Predicate;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import static datomic.Peer.q;
import static datomic.Peer.tempid;
import static datomic.Util.list;
import static datomic.Util.map;
import static datomic.samples.Fns.printQueryResult;
import static datomic.samples.Fns.scratchConnection;
import static datomic.samples.IO.resource;
import static datomic.samples.IO.transactAll;

public class Trust {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        Connection conn = scratchConnection();
        URL url = resource("datomic-java-examples/social-news.edn");
        transactAll(conn, new InputStreamReader(url.openStream()));

        conn.transact(list(map(":db/id", tempid(":db.part/user"),
                               ":story/title", "ElastiCache in 6 minutes",
                               ":story/url", "http://blog.datomic.com/2012/09/elasticache-in-5-minutes.html"),
                           map(":db/id", tempid(":db.part/tx"),
                               "source/confidence", 95L))).get();

        conn.transact(list(map(":db/id", tempid(":db.part/user"),
                               ":story/title", "Request for Urgent Business Relationship",
                               ":story/url", "http://example.com/bogus-url"),
                           map(":db/id", tempid(":db.part/tx"),
                               "source/confidence", 40L))).get();

        Database db = conn.db();

        System.out.println("\nAll stories...");
        printQueryResult(allStories(db));

        System.out.println("\nStories with 90% confidence, by query...");
        printQueryResult(storiesWithHighConfidenceByQuery(db));

        System.out.println("\nStories with 90% confidence, by filter...");
        printQueryResult(storiesWithHighConfidenceByFilter(db));
    }

    public static Collection allStories(Database db) {
        return q("[:find ?title" +
                 " :where [_ :story/title ?title]]", db);
    }

    public static Collection storiesWithHighConfidenceByQuery(Database db) {
        return q("[:find ?title" +
                 " :where [_ :story/title ?title ?tx]" +
                 "        [?tx :source/confidence ?conf]" +
                 "        [(<= 90 ?conf)]]", db);
    }

    /**
     * Returns database filtered to only facts whose transactions have
     * :source/confidence level of 90% or higher
     */
    public static Database filterByConfidence(Database db, final long conf) {
        return db.filter(new Database.Predicate<datomic.Datom>() {
            public boolean apply(Database db, Datom datom) {
                Long confidence = (Long) db.entity(datom.tx()).get(":source/confidence");
                return (confidence != null) && (confidence > conf);
            }
        });
    }

    public static Collection storiesWithHighConfidenceByFilter(Database db) {
        return q("[:find ?title" +
                 " :where [_ :story/title ?title]]", filterByConfidence(db, 90L));
    }

}

