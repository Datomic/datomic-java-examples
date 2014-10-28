package datomic.samples;

import datomic.Peer;

import java.util.Collection;
import java.util.List;

import static datomic.Peer.query;
import static datomic.Util.list;
import static datomic.samples.PrettyPrint.print;

public class With {
    public static Object incorrectHeadCount() {
        return query("[:find (sum ?heads) ." +
                "  :in [[_ ?heads]]]", monsters);
    }

    public static Object correctHeadCount() {
        return query("[:find (sum ?heads) ." +
                " :with ?monster" +
                " :in [[?monster ?heads]]]", monsters);
    }

    public static final Collection monsters = list(list("Cerberus", 3),
                                                   list("Medusa", 1),
                                                   list("Cyclops", 1),
                                                   list("Chimera", 1));
    public static void main(String[] args) {
        print(incorrectHeadCount());
        print(correctHeadCount());
        Peer.shutdown(true);
    }
}
