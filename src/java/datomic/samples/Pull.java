package datomic.samples;

import datomic.Connection;
import datomic.Database;
import datomic.Peer;

import java.util.UUID;

import static datomic.Util.list;
import static datomic.samples.PrettyPrint.print;

public class Pull {
    public static final Object ledZeppelin = list(":artist/gid", UUID.fromString("678d88b2-87b0-403b-b63d-5da7465aecc3"));
    public static final Object mccartney = list(":artist/gid", UUID.fromString("ba550d0e-adac-4864-b88b-407cab5e76af"));
    public static final Object darkSideOfTheMoon = list(":release/gid", UUID.fromString("24824319-9bb8-3d1e-a2c5-b8b864dafd1b"));
    public static final Object dylanHarrisonSessions = list(":release/gid", UUID.fromString("67bbc160-ac45-4caf-baae-a7e9f5180429"));
    public static final Object concertForBanglaDesh = list(":release/gid", UUID.fromString("f3bdff34-9a85-4adc-a014-922eef9cdaa5"));

    public static void main(String[] args) {
        Connection conn = Peer.connect("datomic:free://localhost:4334/mbrainz-1968-1973");
        Database db = conn.db();

        System.out.println("\nPull attribute name");
        print(db.pull("[:artist/name :artist/startYear]", ledZeppelin));

        System.out.println("\nReverse lookup");
        print(db.pull("[:artist/_country]", ":country/GB"));

        System.out.println("\nComponent Defaults");
        print(db.pull("[:release/media]", darkSideOfTheMoon));

        Object dylanHarrisonCD = Peer.query(
                "[:find ?medium ." +
                " :in $ ?release" +
                " :where [?release :release/media ?medium]]",
                db, dylanHarrisonSessions);

        System.out.println("\nReverse Component Lookup");
        print(db.pull("[:release/_media]", dylanHarrisonCD));

        long ghostRiders = (Long) Peer.query(
                        "[:find ?track ." +
                        " :in $ ?release ?trackno" +
                        " :where" +
                        " [?release :release/media ?medium]" +
                        " [?medium :medium/tracks ?track]" +
                        " [?track :track/position ?trackno]]",
                db, dylanHarrisonSessions, 11);

        System.out.println("\nMap specifications");
        print(db.pull("[:track/name {:track/artists [:db/id :artist/name]}]", ghostRiders));

        System.out.println("\nNested Map specifications");
        print(db.pull(
                "[{:release/media" +
                "  [{:medium/tracks" +
                "    [:track/name {:track/artists [:artist/name]}]}]}]",
                concertForBanglaDesh));

        System.out.println("\nWildcard specification");
        print(db.pull("[*]",concertForBanglaDesh));

        System.out.println("\nWildcard + map specification");
        print(db.pull("[* {:track/artists [:artist/name]}]",ghostRiders));

        System.out.println("\nDefault expression");
        print(db.pull("[:artist/name (default :artist/endYear 0)]",mccartney));

        System.out.println("\nDefault expression with different type");
        print(db.pull("[:artist/name (default :artist/endYear \"N/A\")]",mccartney));

        System.out.println("\nAbsent attributes are omitted from results");
        print(db.pull("[:artist/name :died-in-1966?]",mccartney));

        System.out.println("\nLimit plus subspec");
        print(db.pull("[{(limit :track/_artists 10) [:track/name]}]",ledZeppelin));

        System.out.println("\nNo limit");
        print(db.pull("[{(limit :track/_artists nil) [:track/name]}]",ledZeppelin));

        System.out.println("\nPull expression in query");
        print(Peer.query(
                "[:find [(pull ?e [:release/name]) ...]" +
                " :in $ ?artist" +
                " :where [?e :release/artists ?artist]]",
                db, ledZeppelin));

        System.out.println("\nDynamic pattern input");
        print(Peer.query(
                "[:find [(pull ?e pattern) ...]" +
                " :in $ ?artist pattern" +
                " :where [?e :release/artists ?artist]]",
                db, ledZeppelin, "[:release/name]"));
        Peer.shutdown(true);
    }
}
