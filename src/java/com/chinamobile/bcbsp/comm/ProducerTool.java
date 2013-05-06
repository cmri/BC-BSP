/**
 * CopyRight by Chinamobile
 * 
 * ProducerTool.java
 */
package com.chinamobile.bcbsp.comm;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.ObjectMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Producer tool for sending messages to Message Queue.
 * 
 * @author
 * @version
 */
public class ProducerTool extends Thread {

    // For log
    private static final Log LOG = LogFactory.getLog(ProducerTool.class);
    // For time
    private long ConnectTime = 0;/**Clock*/
    private long SendTime = 0;/**Clock*/
    // For retry
    private static final int ReconnectThreshold = 10;
    private int reconnectCount = 0;
    
    private int packSize = 100; //Default 100 messages for a package
    
    private Connection connection = null;
    private Session session = null;
    private Destination destination = null;
    private MessageProducer producer = null;
    private long sleepTime = 500;
    private long timeToLive = 0;
    private String user = ActiveMQConnection.DEFAULT_USER;
    private String password = ActiveMQConnection.DEFAULT_PASSWORD;
    private String url = null; // "tcp://hostName:port"

    private boolean transacted = false;
    private boolean persistent = false;

    private long messageCount = 0; // Total count.
    
    private boolean isFailed = false;

    // Message queue for sending.
    private ConcurrentLinkedQueue<BSPMessage> messageQueue = null;
    
    private String hostNameAndPort = null;
    private boolean newHostNameAndPort = false;
    private String subject = null;
    
    private volatile boolean idle = true;
    
    private volatile boolean noMoreMessagesFlag = false;
    
    private volatile int superStepCounter = -1;
    
    private volatile boolean completed = false;
    
    private Sender sender = null;

    /**
     * Constructor
     * 
     * @param queue
     * @param hostNameAndPort
     * @param subject
     */
    public ProducerTool(ThreadGroup group, int sn, ConcurrentLinkedQueue<BSPMessage> queue,
            String hostNameAndPort, String subject, Sender sender) {

        super(group, "ProducerTool-" + sn);
        
        this.messageQueue = queue;
        this.hostNameAndPort = hostNameAndPort;
        this.newHostNameAndPort = true;
        this.subject = subject; // always = "BSP"
        this.sender = sender;
    }
    
    public void addMessages(ConcurrentLinkedQueue<BSPMessage> messages) {
            messageQueue = messages;
    }

    public void setHostNameAndPort(String hostNameAndPort) {
        if (!hostNameAndPort.equals(this.hostNameAndPort)) {
            this.hostNameAndPort = hostNameAndPort;
            this.newHostNameAndPort = true;
        }
    }
    
    public void setPackSize(int size) {
        this.packSize = size;
    }
    
    public boolean isIdle() {
        return idle;
    }
    
    public void setIdle(boolean state) {
        this.idle = state;
    }
    
    public void setNoMoreMessagesFlag(boolean flag) {
        this.noMoreMessagesFlag = flag;
    }
    
    public int getProgress() {
        return this.superStepCounter;
    }
    
    public void setProgress(int superStepCount) {
        this.superStepCounter = superStepCount - 1;
    }
    
    public void complete() {
        this.completed = true;
    }

    public void showParameters() {
        LOG.info("Connecting to URL: " + url);
        LOG.info("Publishing Messages " + "to queue: " + subject);
        LOG.info("Using "
                + (persistent ? "persistent" : "non-persistent") + " messages");
        LOG.info("Sleeping between publish " + sleepTime + " ms");

        if (timeToLive != 0) {
            LOG.info("Messages time to live " + timeToLive + " ms");
        }
    }

    public void run() {

        while (true) {
            while (this.idle) {
                
                if (this.completed) {
                    return;
                }
                
                if (this.noMoreMessagesFlag) {
                    this.superStepCounter ++;
                    this.noMoreMessagesFlag = false;
                    //LOG.info("Test Progress: from " + (this.superStepCounter - 1) + " to " + this.superStepCounter);
                }
                
                try {
                    Thread.sleep(this.sleepTime);
                } catch (InterruptedException e) {
                    LOG.error("[ProducerTool] to " + this.hostNameAndPort + " has been interrupted for ", e);
                    return;
                }
            }
            if (this.hostNameAndPort == null) {
                LOG.error("Destination hostname is null.");
                return;
            }

            if (this.messageQueue == null) {
                LOG.error("Message queue for ProducerTool is null.");
                return;
            }
            
            this.messageCount = 0;
            this.ConnectTime = 0;
            this.SendTime = 0;

            while (true) {
                
                if (this.reconnectCount == ProducerTool.ReconnectThreshold) {
                    break;
                }

                try {
                    
                    if (this.newHostNameAndPort) { //Should create new connection.
                        
                        if (connection != null) {//Close early connection first.
                            try {
                                connection.close();
                            } catch (Throwable ignore) {
                            }
                        }
                        
                        long start = System.currentTimeMillis();/**Clock*/
                        // Make the destination broker's url.
                        this.url = "tcp://" + this.hostNameAndPort;
                        // Create the connection.
                        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                                user, password, url);
                        connectionFactory.setCopyMessageOnSend(false);
                        connection = connectionFactory.createConnection();
                        connection.start();
                        // Create the session
                        session = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
                        
                        this.ConnectTime += (System.currentTimeMillis() - start);/**Clock*/
                        this.newHostNameAndPort = false;

                        start = System.currentTimeMillis();/**Clock*/
                        destination = session.createQueue(subject);
                        // Create the producer.
                        producer = session.createProducer(destination);
                        if (persistent) {
                            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                        } else {
                            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                        }
                        if (timeToLive != 0) {
                            producer.setTimeToLive(timeToLive);
                        }
                        this.ConnectTime += (System.currentTimeMillis() - start);/**Clock*/
                    }

                    // Start sending messages
                    sendLoop(session, producer);

                    this.idle = true;
                    
                    break;

                } catch (Exception e) {
                    this.reconnectCount ++;
                    if (this.reconnectCount == 1)
                        LOG.error("[ProducerTool] to " + this.hostNameAndPort + " caught: ", e);
                    LOG.info("[ProducerTool] to " + this.hostNameAndPort + " is reconnecting for "
                            + this.reconnectCount + "th time.");
                    LOG.info("---------------- Memory Info ------------------");
                    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
                    MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
                    long used = memoryUsage.getUsed();
                    long committed = memoryUsage.getCommitted();
                    LOG.info("[JVM Memory used] = " + used/1048576 + "MB");
                    LOG.info("[JVM Memory committed] = " + committed/1048576 + "MB");
                    LOG.info("-----------------------------------------------");
                    try {
                        Thread.sleep(this.sleepTime);
                    } catch (InterruptedException e1) {
                        LOG.error("[ProducerTool] caught: ", e1);
                    }
                }//end-try

            }//end-while

            LOG.info("[ProducerTool] to " + this.hostNameAndPort + " has sent " + this.messageCount 
                    + " messages totally! (with " + this.messageQueue.size() + " messages lost!)");
            
            this.sender.addConnectTime(this.ConnectTime);/**Clock*/
            this.sender.addSendTime(this.SendTime);/**Clock*/
            
            if (this.reconnectCount == ProducerTool.ReconnectThreshold) {
                LOG.info("[ProducerTool] to " + this.hostNameAndPort + " has reconnected for "
                        + this.reconnectCount + " times but failed!");
                this.isFailed = true;
                break;
            }
        }//end-while
        
    }//end-run

    public boolean isFailed() {
        return this.isFailed;
    }
    
    private void sendLoop(Session session, MessageProducer producer)
            throws Exception {

        BSPMessage msg;
        int count = 0;
        BSPMessagesPack pack = new BSPMessagesPack();
        ArrayList<BSPMessage> content = new ArrayList<BSPMessage>();
        while ((msg = messageQueue.poll()) != null) {
            content.add(msg);
            count ++;
            this.messageCount ++;
            // enough for a pack.
            if (count == this.packSize) {
                pack.setPack(content);
                long start = System.currentTimeMillis();/**Clock*/
                ObjectMessage message = session.createObjectMessage(pack);
                producer.send(message);
                this.SendTime += (System.currentTimeMillis() - start);/**Clock*/
                content.clear();
                count = 0;
            }
        }//end-while
        // remaining messages into a pack.
        if (content.size() > 0) {
            pack.setPack(content);
            long start = System.currentTimeMillis();/**Clock*/
            ObjectMessage message = session.createObjectMessage(pack);
            producer.send(message);
            this.SendTime += (System.currentTimeMillis() - start);/**Clock*/
            content.clear();
        }
        
    }//end-sendLoop

}
