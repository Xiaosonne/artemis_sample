/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenproject1;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Date;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.naming.NameClassPair;
import javax.naming.NamingEnumeration;

import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.JMSException;
import javax.naming.NamingException;

import javax.jms.DeliveryMode;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.Queue;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.client.ClientSession.AddressQuery;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQBytesMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQMessage;

class RecvThread implements Runnable {

    JMSConsumer consumer;

    public void setComsumer(JMSConsumer cib) {
        this.consumer = cib;
    }

    public void start() {
        System.out.println("Starting ");
        (new Thread(this, "fuck yourself")).start();
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(900);
                Message msg = consumer.receive();
                System.out.println("payLoad = " + msg.toString());
                try {
                    System.out.println("payLoad = " + msg.getBody(String.class));
                    continue;
                } catch (JMSException ex) {
                    Logger.getLogger(RecvThread.class.getName()).log(Level.SEVERE, null, ex);
                }
                System.out.println("payLoad = " + msg.toString());
                if ((msg instanceof ActiveMQBytesMessage)) {
                    ActiveMQBytesMessage msg1 = (ActiveMQBytesMessage) msg;
                    int len = (int) msg1.getBodyLength();
                    byte[] bytes = new byte[len];
                    msg1.readBytes(bytes, len);
                    String str = new String(bytes);
                    System.out.println("payLoad str = " + str);
                }

            } catch (InterruptedException ex) {
                Logger.getLogger(RecvThread.class.getName()).log(Level.SEVERE, null, ex);
                break;
            } catch (JMSException ex) {
                Logger.getLogger(RecvThread.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

}

public class JMSBridgeExample {
    public static void coreExample() throws Exception {
        // ServerLocator locator = ActiveMQClient
        // .createServerLocatorWithoutHA(new
        // TransportConfiguration(connectionfa.class.getName()));

        ServerLocator locator = ActiveMQClient.createServerLocator("tcp://127.0.0.1:61616");
        // In this simple example, we just use one session for both producing and
        // receiving

        ClientSessionFactory factory = locator.createSessionFactory();
        ClientSession session = factory.createSession();

        // A producer is associated with an address ...

        // ClientProducer producer = session.createProducer("example");
        // ClientMessage message = session.createMessage(true);
        // message.getBodyBuffer().writeString("Hello");

        // We need a queue attached to the address ...
        AddressQuery qqExec = session.addressQuery(new SimpleString("execute.#"));
        AddressQuery qqNoti = session.addressQuery(new SimpleString("notice.#"));
        AddressQuery qqRprt = session.addressQuery(new SimpleString("report.#"));
        if (!qqExec.isExists()) {
            session.createQueue(new SimpleString("execute.#"), new SimpleString("servExec"), true);

        }
        if (!qqNoti.isExists()) {
            session.createQueue(new SimpleString("notice.#"), new SimpleString("servNoti"), true);

        }
        if (!qqRprt.isExists()) {
            session.createQueue(new SimpleString("report.#"), new SimpleString("servRprt"), true);
        }
        ClientConsumer consumer = session.createConsumer("servExec");
        ClientConsumer consumer1 = session.createConsumer("servNoti");
        ClientConsumer consumer2 = session.createConsumer("servRprt");

        // Once we have a queue, we can send the message ...

        // producer.send(message);

        // We need to start the session before we can -receive- messages ...
        ClientConsumer[] consumers=new ClientConsumer[]{
            consumer,
            consumer1,
            consumer2
        };
        session.start();
        int i = 0;
        while (true) {
            for (ClientConsumer con : consumers) {
                ClientMessage msgReceived = con.receive(100);
                if (msgReceived != null) {
                    ActiveMQBuffer buf = msgReceived.getBodyBuffer();
                    int bodysize = msgReceived.getBodySize();
                    if (bodysize > 0) {
                        byte[] bytes = new byte[bodysize];
                        buf.readBytes(bytes);
                        System.out.format("queue %s format1 body %d message = %s\r\n",msgReceived.getAddress(), bodysize, new String(bytes));
                    }
                }
            }
           

            if (i > 10000)
                break;
        }

        session.close();
    }

    public static String getString(ByteBuffer buffer) {

        Charset charset = null;

        CharsetDecoder decoder = null;

        CharBuffer charBuffer = null;

        try {

            charset = Charset.forName("UTF-8");

            decoder = charset.newDecoder();

            // 用这个的话，只能输出来一次结果，第二次显示为空

            // charBuffer = decoder.decode(buffer);

            charBuffer = decoder.decode(buffer.asReadOnlyBuffer());

            return charBuffer.toString();

        } catch (Exception ex) {

            ex.printStackTrace();

            return "error";

        }

    }

    public static void topicTest1() throws NamingException, JMSException, InterruptedException {
        Connection connection = null;
        InitialContext initialContext = null;
        try {
            Hashtable<String, Object> properties = new Hashtable<>();
            properties.put("java.naming.factory.initial",
                    "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
            properties.put("connectionFactory.ConnectionFactory", "tcp://127.0.0.1:61616");
            properties.put("queue.queue/execute.#", "execute.#");
            initialContext = new InitialContext(properties);
            Object paires = initialContext.lookup("queue/execute.#");
            System.out.println(paires.toString());
            System.out.println(paires.toString());

        } catch (Exception ex) {
            System.out.println(ex.toString());
        }
    }

    public static void topicTest() throws NamingException, JMSException, InterruptedException {
        Connection connection = null;
        InitialContext initialContext = null;
        try {
            Hashtable<String, Object> properties = new Hashtable<>();
            properties.put("java.naming.factory.initial",
                    "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
            properties.put("connectionFactory.ConnectionFactory", "tcp://127.0.0.1:61616");
            properties.put("queue.queue/test", "test");

            // /Step 1. Create an initial context to perform the JNDI lookup.
            initialContext = new InitialContext(properties);

            // Step 2. perform a lookup on the topic
            Queue topic = (Queue) initialContext.lookup("queue/test");

            // Step 3. perform a lookup on the Connection Factory
            ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

            // Step 4. Create a JMS Connection
            connection = cf.createConnection();

            // Step 5. Create a JMS Session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Step 6. Create a Message Producer
            MessageProducer producer = session.createProducer(topic);

            // Step 7. Create a JMS Message Consumer
            MessageConsumer messageConsumer1 = session.createConsumer(topic);

            // Step 8. Create a JMS Message Consumer
            MessageConsumer messageConsumer2 = session.createConsumer(topic);

            // Step 9. Create a Text Message

            int msgid = 0;
            while (true) {

                Thread.sleep(1000);
                msgid++;
                TextMessage message = session.createTextMessage(String.format("%d msgs ", msgid));
                producer.send(message);
                msgid++;
                message = session.createTextMessage(String.format("%d msgs ", msgid));
                producer.send(message);
                connection.start();

                TextMessage messageReceived = (TextMessage) messageConsumer1.receive(500);
                if (messageReceived != null)
                    System.out.println("Consumer 1 Received message: " + messageReceived.getText());

                messageReceived = (TextMessage) messageConsumer2.receive(500);
                if (messageReceived != null)
                    System.out.println("Consumer 2 Received message: " + messageReceived.getText());

            }

        } catch (InterruptedException ex) {
            Logger.getLogger(JMSBridgeExample.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            // Step 14. Be sure to close our JMS resources!
            if (connection != null) {
                connection.close();
            }

            // Also the initialContext
            if (initialContext != null) {
                initialContext.close();
            }
        }
    }

    public static void sendMessageThrougthTopic() throws InterruptedException {
        // Instantiate the queue
        Topic queue = ActiveMQJMSClient.createTopic("execute.g1");

        // Instantiate the ConnectionFactory (Using the default URI on this case)
        // Also instantiate the jmsContext
        // Using closeable interface
        try (ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory();
                JMSContext jmsContext = cf.createContext()) {
            // Create a message producer, note that we can chain all this into one statement
            JMSProducer producer = jmsContext.createProducer();
            JMSConsumer consumer = jmsContext.createConsumer(queue);

            int i = 0;
            while (true) {
                i++;
                Thread.sleep(1000);
                producer.send(queue, String.format("msg id %d time %s", i, (new Date()).toString()));
                while (true) {
                    try {
                        Message msg = consumer.receive(50);
                        if (msg == null) {
                            break;
                        }
                        if (msg instanceof ActiveMQMessage) {
                            try {
                                System.out.println("payLoad = " + msg.getBody(String.class));
                                continue;
                            } catch (JMSException ex) {
                                Logger.getLogger(RecvThread.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }
                        if ((msg instanceof ActiveMQBytesMessage)) {
                            ActiveMQBytesMessage msg1 = (ActiveMQBytesMessage) msg;
                            int len = (int) msg1.getBodyLength();
                            byte[] bytes = new byte[len];
                            msg1.readBytes(bytes, len);
                            String str = new String(bytes);
                            System.out.println("payLoad str = " + str);
                        }

                    } catch (Exception ex) {
                        Logger.getLogger(RecvThread.class.getName()).log(Level.SEVERE, null, ex);
                        break;
                    }
                }
            }

        }
    }

    public static void main(final String[] args) throws Exception {
        coreExample();
    }
}
