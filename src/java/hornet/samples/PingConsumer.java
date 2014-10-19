package hornet.samples;

import datomic.samples.IO;
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
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.LogManager;

public class PingConsumer {
    public static Map nettyOpts() {
        Map m = new HashMap();
        m.put("port", 5000);
        m.put("host", "localhost");
        m.put("ssl-enabled", true);
        m.put("key-store-path", "datomic/transactor-key.jks");
        m.put("key-store-password", "transactor");
        m.put("trust-store-path", "datomic/transactor-trust.jks");
        m.put("trust-store-password", "transactor");
        return m;
    }
    public static void main(String[] args) {
        SLF4JBridgeHandler.install();
        LogManager.getLogManager().getLogger("").info("Ping Consumer Starting");

        try
        {

            // Step 1. Create the Configuration, and set the properties accordingly
            Configuration configuration = new ConfigurationImpl();
            configuration.setPersistenceEnabled(false);
            configuration.setSecurityEnabled(false);
            configuration.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
            configuration.getAcceptorConfigurations().add(new TransportConfiguration(NettyAcceptorFactory.class.getName(), nettyOpts()));

            // Step 2. Create and start the server
            HornetQServer server = HornetQServers.newHornetQServer(configuration);
            server.start();

            // Step 3. As we are not using a JNDI environment we instantiate the objects directly
            ServerLocator serverLocator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(InVMConnectorFactory.class.getName()));
            ClientSessionFactory sf = serverLocator.createSessionFactory();

            // Step 4. Create a core queue
            ClientSession coreSession = sf.createSession(false, false, false);

            final String queueName = "queue.exampleQueue";

            coreSession.createQueue(queueName, queueName, true);

            coreSession.close();

            ClientSession session = null;

            try
            {
                // Step 5. Create the session, and producer
                session = sf.createSession();

                // Step 7. Create the message consumer and start the connection
                ClientConsumer messageConsumer = session.createConsumer(queueName);
                session.start();

                System.out.println("Waiting for messages...");
                while (true) {

                    // Step 8. Receive the message.
                    ClientMessage messageReceived = messageConsumer.receive();
                    if (messageReceived == null) {
                        System.out.println("Received a null message");
                    } else {
                        messageReceived.acknowledge();
                        System.out.println("Received TextMessage:" + messageReceived.getStringProperty("prop"));
                    }
                }
            }
            finally
            {
                if (session != null) {
                    session.close();
                }

                // Step 9. Be sure to close our resources!
                if (sf != null)
                {
                    sf.close();
                }

                // Step 10. Stop the server
                server.stop();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
