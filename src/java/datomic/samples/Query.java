package datomic.samples;

import datomic.*;

import java.util.List;
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

        System.out.println("Same as previous query, but with explicit db-var ($).");
        results = Peer.query("[:find ?release-name :in $ :where [$ _ :release/name ?release-name]]", db);
        System.out.println(results);
        pause();

        System.out.println("Query that finds releases by artist. Parameterized to accept artist name as an argument.");
        results = Peer.query("[:find ?release-name " +
                              ":in $ ?artist-name " +
                              ":where [?artist :artist/name ?artist-name] " +
                                     "[?release :release/artists ?artist] " +
                                     "[?release :release/name ?release-name]]",
                            db, "John Lennon");
        System.out.println(results);
        pause();

        System.out.println("This query accepts a relation argument as a vector and destructures it within the :in.");
        results = Peer.query("[:find ?release " +
                             " :in $ [?artist-name ?release-name] " +
                             " :where [?artist :artist/name ?artist-name] " +
                             "        [?release :release/artists ?artist] " +
                             "        [?release :release/name ?release-name]]",
                             db, Util.list("John Lennon", "Mind Games"));
        System.out.println(results);
        pause();


        System.out.println("This query accepts a list of artist arguments and uses destructuring to satisfy the " +
                           "query for each of them (union of results).");
        results = Peer.query("[:find ?release-name " +
                              ":in $ [?artist-name ...] " +
                              ":where [?artist :artist/name ?artist-name] " +
                              "       [?release :release/artists ?artist] " +
                              "       [?release :release/name ?release-name]]",
                              db, Util.list("Paul McCartney", "George Harrison"));
        pause();

        System.out.println("This query accepts a list of relations, also a list, and destructures the arguments in " +
                           "the :in clause.");
        results = Peer.query("[:find ?release " +
                              ":in $ [[?artist-name ?release-name]] " +
                              ":where [?artist :artist/name ?artist-name] " +
                              "       [?release :release/artists ?artist] " +
                              "       [?release :release/name ?release-name]]",
                              db, Util.list(Util.list("John Lennon", "Mind Games"),
                                            Util.list("Paul McCartney", "Ram")));
        System.out.println(results);
        pause();
                                              
        System.out.println("This query returns a collection of relations, all artists and their releases.");
        results = Peer.query("[:find ?artist-name ?release-name " +
                              ":where [?release :release/name ?release-name] " +
                              "       [?release :release/artists ?artist] " +
                              "       [?artist :artist/name ?artist-name]]",
                              db);
        System.out.println(results);
        pause();

        System.out.println("This query returns a list of releases by using a find specification.");
        results = Peer.query("[:find [?release-name ...]" +
                             " :in $ ?artist-name " +
                             " :where [?artist :artist/name ?artist-name] " +
                             "        [?release :release/artists ?artist] " +
                             "        [?release :release/name ?release-name]]",
                             db, "John Lennon");
        System.out.println(results);
        pause();

        System.out.println("This query returns values from multiple attributes to provide artist's start date.");
        results = Peer.query("[:find [?year ?month ?day] " +
                             " :in $ ?name " +
                             " :where [?artist :artist/name ?name] " +
                             "        [?artist :artist/startDay ?day] " +
                             "        [?artist :artist/startMonth ?month] " +
                             "        [?artist :artist/startYear ?year]]",
                             db, "John Lennon");
        System.out.println(results);
        pause();

        System.out.println("Return just the year of start date using a scalar find specification.");
        Long year = Peer.query("[:find ?year . " +
                               " :in $ ?name " +
                               " :where [?artist :artist/name ?name] " +
                               "        [?artist :artist/startYear ?year]]",
                               db, "John Lennon");
        System.out.println(year);
        pause();

        System.out.println("Total number of artists who aren't Canadian.");
        Integer res = Peer.query("[:find (count ?eid) . " +
                                  ":where [?eid :artist/name] " +
                                         "(not [?eid :artist/country :country/CA])]",
                                 db);
        System.out.println(res);
        pause();

        System.out.println("Use not-join to bind variable in subquery. Artists w/o releases in 1970.");
        res = Peer.query("[:find (count ?artist) . " +
                          ":where [?artist :artist/name] " +
                          "(not-join [?artist] " +
                          "  [?release :release/artists ?artist] " +
                          "  [?release :release/year 1970])]",
                          db);
        System.out.println(res);
        pause();

        System.out.println("Number of 'Live at Carnegie Hall' releases not by Bill Withers");
        res = Peer.query("[:find (count ?r) . " +
                          ":where [?r :release/name \"Live at Carnegie Hall\"] " +
                                 "(not-join [?r] " +
                                   "[?r :release/artists ?a] " +
                                   "[?a :artist/name \"Bill Withers\"])]",
                         db);
        System.out.println(res);
        pause();

        System.out.println("Total of artists who are a group or female by use of 'or'.");
        res = Peer.query("[:find (count ?artist) . " +
                         " :where (or [?artist :artist/type :artist.type/group] " +
                         "        (and [?artist :artist/type :artist.type/person] " +
                         "             [?artist :artist/gender :artist.gender/female]))]",
                         db);
        System.out.println(res);
        pause();

        System.out.println("Count of releases from Canadian artists or the year 1970 using 'or-join'");
        res = Peer.query("[:find (count ?release) . " +
                         " :where [?release :release/name] " +
                         " (or-join [?release] " +
                         "   (and [?release :release/artists ?artist] " +
                         "        [?artist :artist/country :country/CA]) " +
                         "   [?release :release/year 1970])]",
                         db);
        System.out.println(res);
        pause();


        System.out.println("Track name and duration in minutes using function call in query.");
        results = Peer.query("[:find ?track-name ?minutes " +
                             ":in $ ?artist-name " +
                             ":where [?artist :artist/name ?artist-name] " +
                             "       [?track :track/artists ?artist] " +
                             "       [?track :track/duration ?millis] " +
                             "       [(quot ?millis 60000) ?minutes] " +
                             "       [?track :track/name ?track-name]]",
                             db, "John Lennon");
        System.out.println(results);
        pause();

        // Can't nest expression clauses
        String QueryFail = "[:find ?celsius . " +
                           " :in ?fahrenheit " +
                           " :where [(/ (- ?fahrenheit 32) 1.8) ?celsius]]";
        Integer QueryFailInput = 212;

        System.out.println("Function calls without nesting:");
        Double celsius = Peer.query("[:find ?celsius . " +
                                    " :in ?fahrenheit " +
                                    " :where [(- ?fahrenheit 32) ?f-32] " +
                                    "        [(/ ?f-32 1.8) ?celsius]]",
                         212);
        System.out.println(celsius);
        pause();

        System.out.println("get-else example:");
        results = Peer.query("[:find ?artist-name ?year " +
                             " :in $ [?artist-name ...] " +
                             " :where [?artist :artist/name ?artist-name] " +
                             "        [(get-else $ ?artist :artist/startYear \"N/A\") ?year]]",
                             db, Util.list("Crosby, Stills & Nash", "Crosby & Nash"));
        System.out.println(results);
        pause();

        System.out.println("get-some example:");
        results = Peer.query("[:find [?e ?attr ?name] " +
                             ":in $ ?e " +
                             ":where [(get-some $ ?e :country/name :artist/name) [?attr ?name]]]",
                             db, Util.read(":country/US"));
        System.out.println(results);
        pause();

        System.out.println("fulltext example returns info for songs with 'Jane' in the title.");
        results = Peer.query("[:find ?entity ?name ?tx ?score " +
                             " :in $ ?search " + 
                             " :where [(fulltext $ :artist/name ?search) [[?entity ?name ?tx ?score]]]]",
                             db, "Jane");
        System.out.println(results);
        pause();

        System.out.println("'missing' query finds artists without start year.");
        results = Peer.query("[:find ?name " +
                             " :where [?artist :artist/name ?name] " +
                             "        [(missing? $ ?artist :artist/startYear)]]",
                             db);
        System.out.println(results);
        pause();

        System.out.println("This query finds transactions using the Log API.");
        results = Peer.query("[:find [?tx ...] " +
                             " :in ?log " +
                             " :where [(tx-ids ?log 1000 1050) [?tx ...]]]",
                             conn.log());
        System.out.println(results);
        pause();

        System.out.println("This query finds information about a transaction using the Log API.");
        results = Peer.query("[:find [?e ...] " +
                             " :in ?log ?tx " +
                             " :where [(tx-data ?log ?tx) [[?e]]]]",
                             conn.log(), 13194139534312L);
        System.out.println(results);
        pause();


        System.out.println("Calling a Java static method in a query.");
        results = Peer.query("[:find ?k ?v " +
                             " :where [(System/getProperties) [[?k ?v]]]]");
        System.out.println(results);
        pause();

        System.out.println("Calling a Java instance method in a query.");
        results = Peer.query("[:find ?k ?v " +
                             " :where [(System/getProperties) [[?k ?v]]]" +
                             "        [(.endsWith ?k \"version\")]]");
        System.out.println(results);
        pause();

        System.out.println("Calling Clojure functions");
        results = Peer.query("[:find [?prefix ...] " +
                             " :in [?word ...] " +
                             " :where [(subs ?word 0 5) ?prefix]]",
                             Util.list("hello", "antidisestablishmentarianism"));
        System.out.println(results);
        pause();


        System.out.println("Attr bound by entity ID, query for ident.");
        results = Peer.query("[:find [?aname ...] " +
                             " :where [?attr 42 _] " +
                             "        [?attr :db/ident ?aname]]",
                             db);
        System.out.println(results);
        pause();

        System.out.println("Avoid dynamic attr. specs unless you really need them.");
//        results = Peer.query("[:find [?aname ...] " +
//                             " :in $ [?property ...] " +
//                             " :where [?attr ?property _] " +
//                             "        [?attr :db/ident ?aname]]",
//                             db, Util.read(":db/unique"));
//        System.out.println(results);
//        pause();

        System.out.println("The following three queries are equivalent...");
        System.out.println("This one retrieves an entity by lookup ref.");
        results = Peer.query("[:find [?artist-name ...] " +
                             " :in $ ?country" +
                             " :where [?artist :artist/name ?artist-name] " +
                             "        [?artist :artist/country ?country]]",
                             db, Util.list(Util.read(":country/name"), "Belgium"));
        System.out.println(results);
        pause();

        System.out.println("This one retrieves the same entity by ident.");
        results = Peer.query("[:find [?artist-name ...] " +
                             " :in $ ?country" +
                             " :where [?artist :artist/name ?artist-name] " +
                             "        [?artist :artist/country ?country]]",
                             db, Util.read(":country/BE"));
        System.out.println(results);
        pause();

        System.out.println("This one retrieves the same entity by its entity ID (Long)");
        results = Peer.query("[:find [?artist-name ...] " +
                             " :in $ ?country" +
                             " :where [?artist :artist/name ?artist-name] " +
                             "        [?artist :artist/country ?country]]",
                             db, 17592186045438L);
        System.out.println(results);
        pause();

        System.out.println("This query doesn't work as intended.");
        results = Peer.query("[:find [?artist-name ...] " +
                             " :in $ ?country [?reference ...] " +
                             " :where [?artist :artist/name ?artist-name] " +
                             "        [?artist ?reference ?country]]",
                             db, Util.read(":country/BE"), Util.list(Util.read(":artist/country")));

        System.out.println(results);
        pause();

        System.out.println("We fix it by manually resolving the ident to its entity id.");
//      results = Peer.query("[:find [?artist-name ...] " +
//                           " :in $ ?country [?reference ...] " +
//                           " :where [(datomic.api/entid $ ?country) ?country-id] " +
//                           "        [?artist :artist/name ?artist-name] " +
//                           "        [?artist ?reference ?country-id]]",
//                           db, Util.read(":country/BE"), Util.list(Util.read(":artist/country")));
//      System.out.println(results);
//      pause();

        System.out.println("We have to use ':with' to get duplicate values as Datomic query returns sets by default.");
        Long heads = Peer.query("[:find (sum ?heads) . " +
                                " :in [[_ ?heads]]]",
                                Util.list(Util.list("Cerberus", 3),
                                          Util.list("Cyclops", 1),
                                          Util.list("Medusa", 1),
                                          Util.list("Chimera", 1)));
        System.out.println("First, incorrect results: ");
        System.out.println(heads);
        pause();

        System.out.println("This time, fixed with ':with': ");
        heads = Peer.query("[:find (sum ?heads) . " +
                           " :with ?monster " +
                           " :in [[?monster ?heads]]]",
                           Util.list(Util.list("Cerberus", 3),
                                     Util.list("Cyclops", 1),
                                     Util.list("Medusa", 1),
                                     Util.list("Chimera", 1)));
        System.out.println(heads);
        pause();

        // Aggregate examples
        System.out.println("Using 'max' and 'min' aggregates to get shortest and longest durations.");
        results = Peer.query("[:find [(min ?dur) (max ?dur)] " +
                             " :where [_ :track/duration ?dur]]",
                             db);
        System.out.println(results);
        pause();

        System.out.println("Sum of all track counts.");
        Long sum = Peer.query("[:find (sum ?count) . " +
                              " :with ?medium " +
                              " :where [?medium :medium/trackCount ?count]]",
                              db);
        System.out.println(sum);
        pause();

        System.out.println("Counting with and without count-distinct: ");
        results = Peer.query("[:find [(count ?name) (count-distinct ?name)] " +
                             " :with ?artist " +
                             " :where [?artist :artist/name ?name]]",
                             db);
        System.out.println(results);
        pause();

        System.out.println("Count of tracks.");
        results = Peer.query("[:find (count ?track) " +
                             " :where [?track :track/name]]",
                             db);
        System.out.println(results);
        pause();

        System.out.println("Some basic stats...");
        results = Peer.query("[:find ?year (median ?namelen) (avg ?namelen) (stddev ?namelen) " +
                             " :with ?track " +
                             " :where [?track :track/name ?name] " +
                             "        [(count ?name) ?namelen] " +
                             "        [?medium :medium/tracks ?track] " +
                             "        [?release :release/media ?medium] " +
                             "        [?release :release/year ?year]]",
                             db);
        System.out.println(results);
        pause();


        System.out.println("Set of distinct values...");
        results = Peer.query("[:find (distinct ?v) . " +
                             " :in [?v ...]] ",
                             Util.list(1, 1, 2, 2, 2, 3));
        System.out.println(results);
        pause();

        System.out.println("Five shortest and five longest track durations in milliseconds.");
        results = Peer.query("[:find [(min 5 ?millis) (max 5 ?millis)] " +
                             " :where [?track :track/duration ?millis]]",
                             db);
        System.out.println(results);
        pause();

        System.out.println("Two random names using rand (duplicates tolerated) and sample (only distinct)");
        results = Peer.query("[:find [(rand 2 ?name) (sample 2 ?name)] " +
                             " :where [_ :artist/name ?name]]",
                             db);
        System.out.println(results);
        pause();

        System.out.println("Using a QueryRequest to return a relation");
        QueryRequest queryRequest = QueryRequest.create("[:find ?artist-name ?release-name " +
                                                        " :in $ ?artist-name " +
                                                        " :where [?artist :artist/name ?artist-name] " +
                                                        "        [?release :release/artists ?artist] " +
                                                        "        [?release :release/name ?release-name]]",
                                                        db, "John Lennon");
        Collection<List<String>> queryResultRelation = Peer.query(queryRequest);

        System.out.println(queryResultRelation);
        pause();

        System.out.println("Using a QueryRequest to return a collection");
        queryRequest = QueryRequest.create("[:find [?release-name ...]" +
                                           " :in $ ?artist-name " +
                                           " :where [?artist :artist/name ?artist-name] " +
                                           "        [?release :release/artists ?artist] " +
                                           "        [?release :release/name ?release-name]]",
                                           db, "John Lennon");
        Collection<String> queryResultCollection = Peer.query(queryRequest);

        System.out.println(queryResultCollection);
        pause();

        System.out.println("Using a QueryRequest to return a single tuple");
        queryRequest = QueryRequest.create("[:find [?year ?month ?day]" +
                                           " :in $ ?name" +
                                           " :where [?artist :artist/name ?name] " +
                                           "        [?artist :artist/startDay ?day] " +
                                           "        [?artist :artist/startMonth ?month] " +
                                           "        [?artist :artist/startYear ?year]]",
                                           db, "John Lennon");
        List<Long> queryResultSingleTuple = Peer.query(queryRequest);

        System.out.println(queryResultSingleTuple);
        pause();


        System.out.println("Using a QueryRequest to return a single scalar");
        queryRequest = QueryRequest.create("[:find ?year . " +
                                           " :in $ ?name " +
                                           " :where [?artist :artist/name ?name] " +
                                           "        [?artist :artist/startYear ?year]]",
                                           db, "John Lennon");
        Long queryResultSingleScalar = Peer.query(queryRequest);

        System.out.println(queryResultSingleScalar);
        pause();


        System.out.println("Timeout a long running query.");
        queryRequest = QueryRequest.create("[:find ?track-name " +
                                           " :in $ ?artist-name " +
                                           " :where [?track :track/artists ?artist] " +
                                           "        [?track :track/name ?track-name] " +
                                           "        [?artist :artist/name ?artist-name]]",
                                           db, "John Lennon").timeout(100);

        Exception error = null;
        try {
            Peer.query(queryRequest);
        } catch (Exception e) {
            error = e;
            if (error.getMessage().contains("Query canceled")) {
                System.out.println("Caught expected timeout exception: " + error.getMessage());
            } else {
                throw new RuntimeException(e);
            }
        }
        if (error == null) {
            System.out.println("Expected query to timeout.");
        }
    }

    private static final Scanner scanner = new Scanner(System.in);

    private static void pause() {
        if (System.getProperty("NOPAUSE") == null) {
            System.out.println("\nPress enter to continue...");
            scanner.nextLine();

        }
    }
}

