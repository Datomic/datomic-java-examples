package datomic.samples;


import datomic.Peer;

import datomic.Connection;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;

public class Fns {
    /**
     * Returns single item in the collection, null if collection empty, throws if collection
     * has more than one item.
     */
    public static Object solo(Object c) {
        if (c == null) return null;
        Iterator it = ((Collection)c).iterator();
        if (!it.hasNext()) return null;
        Object result = it.next();
        if (it.hasNext()) throw new RuntimeException("Expected one item, got more than one");
        return result;
    }

    /**
     * Connection to a fresh in-memory Datomic database.
     * @return
     */
    public static Connection scratchConnection() {
        String uri = "datomic:mem://" + UUID.randomUUID();
        Peer.createDatabase(uri);
        return Peer.connect(uri);
    }

}
