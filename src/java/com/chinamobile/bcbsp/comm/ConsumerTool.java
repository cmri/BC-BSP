/**
 * CopyRight by Chinamobile
 * 
 * ComsumerTool.java
 */
package com.chinamobile.bcbsp.comm;

import java.io.IOException;
import java.util.Iterator;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.ObjectMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Consumer tool for receiving messages from Message Queue.
 * 
 * @author
 * @version
 */
public class ConsumerTool extends Thread implements MessageListener,
        ExceptionListener {

    // For log
    private static final Log LOG = LogFactory.getLog(ConsumerTool.class);
    
    private boolean running;

    private Session session;
    private Destination destination;
    private MessageConsumer consumer;
    private MessageProducer replyProducer;

    private boolean pauseBeforeShutdown = false;
    private int maxiumMessages = 0; // If >0, end consumer based on max message
                                    // number.
    private String subject = "BSP.DEFAULT"; // Should be jobID, if jobs's no =
                                            // 1, default is ok.
    private boolean topic = false;
    private String user = ActiveMQConnection.DEFAULT_USER;
    private String password = ActiveMQConnection.DEFAULT_PASSWORD;
    private String url = null; // "failover://vm://brokerName"
    private boolean transacted = false;
    private boolean durable = false;
    private String clientId;
    private int ackMode = Session.DUPS_OK_ACKNOWLEDGE;
    private String consumerName = "BSPConsumer";
    private long sleepTime = 0;
    private long receiveTimeOut = 1000; // If < 0, set consuming mode to
                                       // listener.
    private long batch = 500; // Default batch size for CLIENT_ACKNOWLEDGEMENT or
                             // SESSION_TRANSACTED
    private long messagesReceived = 0;
    
    private long messageCount = 0;

    private MessageQueuesInterface messageQueues;
    //brokerName for this staff to receive messages.
    private String brokerName;

    private Receiver receiver = null;

    /**
     * Constructor
     * 
     * @param incomingQueues
     * @param subject1
     * @param subject2
     */
    public ConsumerTool(
            Receiver aReceiver, MessageQueuesInterface messageQueues,
            String brokerName, String subject) {
        this.receiver = aReceiver;
        this.messageQueues = messageQueues;
        this.brokerName = brokerName;
        this.subject = subject;
    }

    public void showParameters() {
        System.out.println("Connecting to URL: " + url);
        System.out.println("Consuming " + (topic ? "topic" : "queue") + ": "
                + subject);
        System.out.println("Using a " + (durable ? "durable" : "non-durable")
                + " subscription");
    }

    public void run() {
        try {
            running = true;

            this.url = "failover://vm://" + this.brokerName;
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                    user, password, url);
            connectionFactory.setOptimizeAcknowledge(true);
            Connection connection = connectionFactory.createConnection();
            if (durable && clientId != null && clientId.length() > 0
                    && !"null".equals(clientId)) {
                connection.setClientID(clientId);
            }
            connection.setExceptionListener(this);
            connection.start();

            session = connection.createSession(transacted, ackMode);
            if (topic) {
                destination = session.createTopic(subject);
            } else {
                destination = session.createQueue(subject);
            }

            replyProducer = session.createProducer(null);
            replyProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            if (durable && topic) {
                consumer = session.createDurableSubscriber(
                        ( Topic ) destination, consumerName);
            } else {
                consumer = session.createConsumer(destination);
            }

            consumeMessages(connection, session, consumer, receiveTimeOut);
            LOG.info("[ConsumerTool] has received " + this.messagesReceived
                    + " object messages from <" + this.subject + ">.");
            LOG.info("[ConsumerTool] has received " + this.messageCount
                    + " BSP messages from <" + this.subject + ">.");
            
            this.receiver.addMessageCount(this.messageCount);

        } catch (Exception e) {
            LOG.error("[ConsumerTool] caught: ", e);
        }
    }

    public void onMessage(Message message) {

        messagesReceived++;

        try {

            if (message instanceof ObjectMessage) {

                ObjectMessage objMsg = ( ObjectMessage ) message;

                BSPMessagesPack msgPack = ( BSPMessagesPack ) objMsg.getObject();
                
                BSPMessage bspMsg;
                Iterator<BSPMessage> iter = msgPack.getPack().iterator();
                while (iter.hasNext()) {
                    bspMsg = iter.next();
                    String vertexID = bspMsg.getDstVertexID();
                    this.messageQueues.incomeAMessage(vertexID, bspMsg);
                    this.messageCount ++;
                }

            } else {
                // Message received is not ObjectMessage.
                LOG.error("[ConsumerTool] Message received is not ObjectMessage!");
            }

            if (message.getJMSReplyTo() != null) {
                replyProducer.send(message.getJMSReplyTo(), session
                        .createTextMessage("Reply: "
                                + message.getJMSMessageID()));
            }

            if (transacted) {
                if ((messagesReceived % batch) == 0) {
                    System.out.println("Commiting transaction for last "
                            + batch + " messages; messages so far = "
                            + messagesReceived);
                    session.commit();
                }
            } else if (ackMode == Session.CLIENT_ACKNOWLEDGE) {
                if ((messagesReceived % batch) == 0) {
                    System.out.println("Acknowledging last " + batch
                            + " messages; messages so far = "
                            + messagesReceived);
                    message.acknowledge();
                }
            }

        } catch (JMSException e) {
            LOG.error("[ConsumerTool] caught: ", e);
        } finally {
            if (sleepTime > 0) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    
                }
            }
        }
    }

    public synchronized void onException(JMSException ex) {
        System.out.println("[" + this.getName()
                + "] JMS Exception occured.  Shutting down client.");
        running = false;
    }

    synchronized boolean isRunning() {
        return running;
    }

    protected void consumeMessagesAndClose(Connection connection,
            Session session, MessageConsumer consumer) throws JMSException,
            IOException {
        System.out.println("[" + this.getName()
                + "] We are about to wait until we consume: " + maxiumMessages
                + " message(s) then we will shutdown");

        for (int i = 0; i < maxiumMessages && isRunning();) {
            Message message = consumer.receive(1000);
            if (message != null) {
                i++;
                onMessage(message);
            }
        }
        System.out.println("[" + this.getName() + "] Closing connection");
        consumer.close();
        session.close();
        connection.close();
        if (pauseBeforeShutdown) {
            System.out.println("[" + this.getName()
                    + "] Press return to shut down");
            System.in.read();
        }
    }

    protected void consumeMessagesAndClose(Connection connection,
            Session session, MessageConsumer consumer, long timeout)
            throws JMSException, IOException {
        System.out
                .println("["
                        + this.getName()
                        + "] We will consume messages while they continue to be delivered within: "
                        + timeout + " ms, and then we will shutdown");

        Message message;
        while ((message = consumer.receive(timeout)) != null) {
            onMessage(message);
        }

        System.out.println("[" + this.getName() + "] Closing connection");
        consumer.close();
        session.close();
        connection.close();
    }

    protected void consumeMessages(Connection connection, Session session,
            MessageConsumer consumer, long timeout) throws JMSException,
            IOException {
        Message message;

        while (!this.receiver.getNoMoreMessagesFlag()) {
            while ((message = consumer.receive(timeout)) != null) {
                onMessage(message);
            }
        }
        consumer.close();
        session.close();
        connection.close();
    }

    public void setAckMode(String ackMode) {
        if ("CLIENT_ACKNOWLEDGE".equals(ackMode)) {
            this.ackMode = Session.CLIENT_ACKNOWLEDGE;
        }
        if ("AUTO_ACKNOWLEDGE".equals(ackMode)) {
            this.ackMode = Session.AUTO_ACKNOWLEDGE;
        }
        if ("DUPS_OK_ACKNOWLEDGE".equals(ackMode)) {
            this.ackMode = Session.DUPS_OK_ACKNOWLEDGE;
        }
        if ("SESSION_TRANSACTED".equals(ackMode)) {
            this.ackMode = Session.SESSION_TRANSACTED;
        }
    }

    public void setClientId(String clientID) {
        this.clientId = clientID;
    }

    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public void setMaxiumMessages(int maxiumMessages) {
        this.maxiumMessages = maxiumMessages;
    }

    public void setPauseBeforeShutdown(boolean pauseBeforeShutdown) {
        this.pauseBeforeShutdown = pauseBeforeShutdown;
    }

    public void setPassword(String pwd) {
        this.password = pwd;
    }

    public void setReceiveTimeOut(long receiveTimeOut) {
        this.receiveTimeOut = receiveTimeOut;
    }

    public void setSleepTime(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public void setTopic(boolean topic) {
        this.topic = topic;
    }

    public void setQueue(boolean queue) {
        this.topic = !queue;
    }

    public void setTransacted(boolean transacted) {
        this.transacted = transacted;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setBatch(long batch) {
        this.batch = batch;
    }
}
