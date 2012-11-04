package datomic.samples;

import datomic.Database;

import static datomic.Peer.q;
import static datomic.Util.read;
import static datomic.samples.Fns.solo;

public class Schema {
    public static final Object CARDINALITY_ONE = read(":db.cardinality/one");
    public static final Object CARDINALITY_MANY = read(":db.cardinality/many");

    public static Object cardinality(Object db, Object attr) {
        return solo(solo(q("[:find ?v " +
                           ":in $ ?attr " +
                           ":where " +
                           "[?attr :db/cardinality ?card] " +
                           "[?card :db/ident ?v]]", db, attr)));
    }
}
