package datomic.samples;

import datomic.Connection;
import datomic.Util;

import java.io.Reader;
import java.net.URI;
import java.net.URL;
import java.util.Iterator;
import java.util.List;

public class IO {
    public static void transactAll(Connection conn, Reader reader) {
        List<List> txes = Util.readAll(reader);
        for (java.util.Iterator<List> it = txes.iterator(); it.hasNext(); ) {
            List tx =  it.next();
            conn.transact(tx);
        }
    }

    public static URL resource(String name) {
        return Thread.currentThread().getContextClassLoader().getResource(name);
    }
}
