package datomic.samples;

import java.util.Collection;

import static datomic.Peer.q;
import static datomic.Util.list;
import static datomic.samples.Fns.printQueryResult;

public class With {
    public static Collection incorrectHeadCount() {
        return q("[:find (sum ?heads)" +
                "  :in [[_ ?heads]]]", monsters);
    }

    public static Collection correctHeadCount() {
        return q("[:find (sum ?heads)" +
                "  :with ?monster" +
                "  :in [[?monster ?heads]]]", monsters);
    }

    public static final Collection monsters = list(list("Cerberus", 3),
                                                   list("Medusa", 1),
                                                   list("Cyclops", 1),
                                                   list("Chimera", 1));
    public static void main(String[] args) {
        printQueryResult(incorrectHeadCount());
        printQueryResult(correctHeadCount());
    }
}
