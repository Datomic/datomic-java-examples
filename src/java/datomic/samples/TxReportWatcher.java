package datomic.samples;

import datomic.Connection;
import datomic.Datom;

import java.util.*;
import java.util.concurrent.BlockingQueue;

import static datomic.Connection.DB_AFTER;
import static datomic.Peer.connect;
import static datomic.Connection.TX_DATA;
import static datomic.Peer.q;

public class TxReportWatcher {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: TxReortWatcher datomic-uri datomic-attr");
            System.exit(-1);
        }
        watchTxReports(args[0], args[1]);
    }

    public static final String byAttribute = "[:find ?entity ?value" +
                                             " :in $ ?attribute" +
                                             " :where [?entity ?attribute ?value]]";

    private static void watchTxReports(String url, String attr) {
        final Connection conn = connect(url);
        final Object attrid = conn.db().entid(attr);
        if (attrid == null) {
            throw new IllegalArgumentException("No attribute named " + attr);
        }
        final BlockingQueue<Map> queue = conn.txReportQueue();
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        final Map tx = queue.take();
                        Collection<List<Object>> results = q(byAttribute, tx.get(TX_DATA), attrid);
                        for (Iterator<List<Object>> iterator = results.iterator(); iterator.hasNext(); ) {
                            printList(iterator.next());
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).run();
    }

    private static void printList(List<Object> next) {
        for (java.util.Iterator iterator = next.iterator(); iterator.hasNext(); ) {
            System.out.print(iterator.next());
            System.out.print(" ");
        }
        System.out.println("");
    }
}
