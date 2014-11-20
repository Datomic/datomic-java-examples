package datomic.samples;

import datomic.Database;

import static datomic.Peer.query;
import static datomic.Util.read;

public class Schema {
    public static final Object CARDINALITY_ONE = read(":db.cardinality/one");
    public static final Object CARDINALITY_MANY = read(":db.cardinality/many");

    public static Object cardinality(Object db, Object attr) {
        return query("[:find ?v . " +
                      ":in $ ?attr " +
                      ":where " +
                      "[?attr :db/cardinality ?card] " +
                      "[?card :db/ident ?v]]", db, attr);
    }
}
