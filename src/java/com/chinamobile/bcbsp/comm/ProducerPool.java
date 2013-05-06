/**
 * CopyRight by Chinamobile
 * 
 * ProducerPool.java
 */
package com.chinamobile.bcbsp.comm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ProducerTools Pool
 * 
 * @author Bai Qiushi
 * @version 1.0 2012-3-29
 */
public class ProducerPool extends ThreadGroup {

    // For log
    private static final Log LOG = LogFactory.getLog(ProducerPool.class);
    
    private int nextSerialNumber = 0;
    private int maxProducerNum;
    @SuppressWarnings("unused")
    private String jobID;
    
    /** HostName:Port--ProducerTool */
    private HashMap<String, ProducerTool> producers = null;
    
    public ProducerPool(int maxNum, String jobID) {
        super("ProducerTools Pool");
        this.maxProducerNum = maxNum;
        this.jobID = jobID;
        this.producers = new HashMap<String, ProducerTool>();
    }
    
    /**
     * Tell all producers no more messages to finish them.
     */
    public void finishAll() {
        Thread[] producers = getAllThread();
        for (int i = 0; i < producers.length; i++) {
            if (!(producers[i] instanceof ProducerTool)) {
                continue;
            }
            ProducerTool p = (ProducerTool) producers[i];
            
            p.setNoMoreMessagesFlag(true);
        }
    }
    
    public void completeAll() {
        Thread[] producers = getAllThread();
        for (int i = 0; i < producers.length; i++) {
            if (!(producers[i] instanceof ProducerTool)) {
                continue;
            }
            ProducerTool p = (ProducerTool) producers[i];
            
            p.complete();
        }
    }
    
    /**
     * @param superStepCount
     * @return The number of ProducerTool that are alive.
     */
    public int getActiveProducerCount(int superStepCount) {
        Thread[] producers = getAllThread();
        int count = 0;
        for (int i = 0; i < producers.length; i++) {
            if ((producers[i] instanceof ProducerTool)
                    && ((ProducerTool) producers[i]).getProgress() < superStepCount) {
                count++;
            }
        }
        return count;
    }

    
    public void setActiveProducerProgress(int superStepCount) {
        Thread[] producers = getAllThread();
        for (int i = 0; i < producers.length; i++) {
            try {
                if (producers[i] instanceof ProducerTool) {
                    ((ProducerTool) producers[i]).setProgress(superStepCount);
                }
            } catch (Exception e) {
                // Ingore this.
            }
        }
    }
    
    public void cleanFailedProducer() {
        ArrayList<String> failRecord = new ArrayList<String>();
        for (Entry<String, ProducerTool> entry: this.producers.entrySet()) {
            if (entry.getValue().isFailed()) {
                failRecord.add(entry.getKey());
            }
        }
        
        for (String str: failRecord) {
            this.producers.remove(str);
        }
    }
    
    /**
     * @return The number of ProducerTool. This may include killed Threads that
     *         were not replaced.
     */
    public int getProducerCount() {
        Thread[] producers = getAllThread();
        int count = 0;
        for (int i = 0; i < producers.length; i++) {
            if ((producers[i] instanceof ProducerTool)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Obtain a free ProducerTool to send messages.
     * Give preference to the ProducerTool for the same destination.
     * 
     * @param hostNameAndPort
     * @param superStepCount
     * @param sender
     * @return ProducerTool
     */
    public ProducerTool getProducer(String hostNameAndPort, int superStepCount, Sender sender) {
        
        ProducerTool pt = null;
        
        if (producers.containsKey(hostNameAndPort)) {
            pt = producers.get(hostNameAndPort);
        }
        else {
            int count = this.getActiveProducerCount(superStepCount-1);
            if (count < this.maxProducerNum) {
                pt = startNewProducer(hostNameAndPort, superStepCount, sender);
                this.producers.put(hostNameAndPort, pt);
            }
            else {
                while ((pt = getAnIdleProducer()) == null) {
                    try {
                        LOG.info("[ProducerPool] is sleeping to wait to get an idle producer...");
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOG.error("[ProducerPool] caught: ", e);
                        return null;
                    }
                }
                pt.setHostNameAndPort(hostNameAndPort);
            }
        }
        
        return pt;
    }

    /**
     * get all threads in pool.
     * 
     * @return
     */
    private Thread[] getAllThread() {
        Thread[] threads = new Thread[activeCount()];
        this.enumerate(threads);
        return threads;
    }
    
    /**
     * get an idle producer.
     * @return ProduerTool or null when no one is idle.
     */
    private ProducerTool getAnIdleProducer() {
        ProducerTool pt = null;
        Thread[] producers = getAllThread();
        for (int i = 0; i < producers.length; i ++) {
            if (producers[i] instanceof ProducerTool) {
                ProducerTool p = (ProducerTool) producers[i];
                if (p.isIdle()) {
                    pt = p;
                    return pt;
                }
            }
        }
        return pt;
    }

    /**
     * Create a new ProducerTool
     * @param hostNameAndPort
     * @param superStepCount
     */
    private synchronized ProducerTool startNewProducer(String hostNameAndPort, int superStepCount, Sender sender) {
        ProducerTool newProducer = new ProducerTool(this, this.nextSerialNumber++, 
                null, hostNameAndPort, "BSP", sender);
        newProducer.setProgress(superStepCount);
        newProducer.start();
        LOG.info("[ProducerPool] has started a new ProducerTool to: " + hostNameAndPort);
        return newProducer;
    }

}
