package datomic.samples;

import datomic.Connection;
import datomic.Database;

import java.util.*;

import static datomic.Peer.*;
import static datomic.Util.read;
import static datomic.samples.Fns.scratchConnection;
import static datomic.samples.Schema.CARDINALITY_MANY;
import static datomic.samples.Schema.CARDINALITY_ONE;
import static datomic.samples.Schema.cardinality;

public class MaybeQuery {

    /**
     * Function intended for use inside a Datomic query
     *
     * @param db      a database value
     * @param e       an entity id
     * @param attr    an attribute
     * @param ifNot   value to return if attribute absent
     * @return        attribute value, set of attribute values, or ifNot, depending
     *                on cardinality of the attribute, and whether any values are present
     */
    public static Object maybe(Object db, Object e, Object attr, Object ifNot) {
        Collection<List<Object>> result = query("[:find ?v " +
                                                 ":in $ ?e ?a " +
                                                 ":where [?e ?a ?v]]", db, e, attr);
        if (result.isEmpty()) {
            return ifNot;
        } else {
            Object card = cardinality(db, attr);
            if (card.equals(CARDINALITY_ONE)) return result.iterator().next(); // only one in list
            Set acc = new HashSet();
            for (Iterator<List<Object>> iterator = result.iterator(); iterator.hasNext(); ) {
                acc.add(iterator.next());
            }
            return Collections.unmodifiableSet(acc);
        }
    }

    public static String rules  =
            "[[[attr-in-namespace ?e ?ns2]\n" +
            "  [?e :db/ident ?a]\n" +
            "  [?e :db/valueType]\n" +
            "  [(namespace ?a) ?ns1]\n" +
            "  [(= ?ns1 ?ns2)]]]";

    public static void main(String[] args) {
        Connection conn = scratchConnection();
        Database db = conn.db();

        System.out.println("Maybe entity zero has a docstring...");
        System.out.println(maybe(db, 0, read(":db/doc"), "<docstring missing>"));

        System.out.println("\nMaybe entity zero has installed some atttributes");
        System.out.println(maybe(db, 0, read(":db.install/attribute"), "<no attributes installed>"));

        System.out.println("\nMaybe entity one has installed some attributes");
        System.out.println(maybe(db, 1, read(":db.install/attribute"), "<no attributes installed>"));

        System.out.println("\nMaybe we can call maybe from inside a query");
        System.out.println(q("[:find ?ident ?card" +
                             " :where [?e :db/ident ?ident]" +
                             "        [(datomic.samples.MaybeQuery/maybe $ ?e :db/cardinality \"<none>\") ?card]]",
                           db));

        System.out.println("Look up attributes in the db namespace:");
        System.out.println(q(
                "[:find ?e ?ident\n" +
                "     :in $ %\n" +
                "     :where\n" +
                "     (attr-in-namespace ?e \"db\")\n" +
                "     [?e :db/ident ?ident]]",
                db, rules));
    }
}
