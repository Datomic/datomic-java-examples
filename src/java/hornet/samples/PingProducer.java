package hornet.samples;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class PingProducer {
    public static Map nettyOpts() {
        Map m = new HashMap();
        m.put("port", 5000);
        m.put("host", "localhost");
        m.put("ssl-enabled", true);
        m.put("key-store-path", "datomic/transactor-key.jks");
        m.put("key-store-password", "transactor");
        return m;
    }

    public static void main(final String[] args) throws InterruptedException {
        SLF4JBridgeHandler.install();
        LogManager.getLogManager().getLogger("").info("Ping Producer Starting");
        try
        {
            // Step 3. As we are not using a JNDI environment we instantiate the objects directly
            final String queueName = "queue.exampleQueue";

            ServerLocator serverLocator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName(), nettyOpts()));
            for (int n=0; true; n++) {
                ClientSessionFactory sf = serverLocator.createSessionFactory();

                // Step 5. Create the session, and producer
                ClientSession session = sf.createSession();

                ClientProducer producer = session.createProducer(queueName);

                // Step 6. Create and send a message
                ClientMessage message = session.createMessage(false);

                message.putStringProperty("prop", "ping " + n);

                System.out.println("Sending the message " + n);

                producer.send(message);

                System.out.println("Close the session");
                session.close();
                sf.close();
                // serverLocator.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
        System.out.println("The end");
    }
}
