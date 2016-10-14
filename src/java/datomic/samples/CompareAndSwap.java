package datomic.samples;

import datomic.Connection;
import static datomic.Connection.*;

import datomic.Database;
import datomic.ListenableFuture;
import datomic.Peer;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static datomic.Util.*;

import static datomic.samples.Fns.scratchConnection;
import static datomic.samples.IO.resource;
import static datomic.samples.IO.transactAll;
import static datomic.Peer.tempid;

public class CompareAndSwap {
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        Connection conn = scratchConnection();
        URL url = resource("datomic-java-examples/accounts.edn");
        transactAll(conn, new InputStreamReader(url.openStream()));

        Object account = tempid(":db.part/user");
        Map txResult = conn.transact(list(map(":db/id", account, ":account/balance", 100))).get();
        account = Peer.resolveTempid((Database)txResult.get(DB_AFTER), txResult.get(TEMPIDS),  account);

        System.out.println("CAS from 100->110 should succeed");
        conn.transact(list(list(":db.fn/cas", account, ":account/balance", 100, 110))).get();

        System.out.println("CAS from 100->120 should fail");
        try {
            conn.transact(list(list(":db.fn/cas", account, ":account/balance", 100, 120))).get();
        } catch (Throwable t) {
            System.out.println("Failed with " + t.getMessage());
        }

        System.out.println("Balance is " + conn.db().entity(account).get(":account/balance"));

    }
}
