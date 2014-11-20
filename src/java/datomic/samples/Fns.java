package datomic.samples;


import datomic.Peer;

import datomic.Connection;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;

public class Fns {
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
