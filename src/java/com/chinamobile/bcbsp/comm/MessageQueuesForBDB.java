/**
 * CopyRight by Chinamobile
 * 
 * MessageQueuesForBDB.java
 */
package com.chinamobile.bcbsp.comm;

import java.io.File;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;



/**
 * @author root
 * Message queues manager for BerkeleyDB Java Edition supported.
 */

public class MessageQueuesForBDB implements MessageQueuesInterface {
    @SuppressWarnings("unused")
    private static final Log LOG = LogFactory.getLog(MessageQueuesForBDB.class);

    private BSPJobID jobID;
    private int partitionID;

 
    private File fileRoot;
    private File messagesDataFile;
    
    private int nextOutgoingQueueCount = 1;
    private  ConcurrentHashMap<String, ConcurrentLinkedQueue<BSPMessage>> outgoingQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<BSPMessage>>();
    
    private BDBMap<String, String> incomingQueues = null;
    private BDBMap<String, String> incomedQueues = null;
    class BSPMessageWithUniqueID{
        String msg = null;
        String uniqueID = null;
        public BSPMessageWithUniqueID(String msg, String uniqueID) {
            this.msg = msg;
            this.uniqueID = uniqueID;
        }
    }
    public MessageQueuesForBDB(BSPJob job, int partitionID){
        


        this.jobID = job.getJobID();
        this.partitionID = partitionID;
        
        this.fileRoot = new File("/tmp/bcbsp/messagedata/" + this.jobID.toString() + "/"
                + "partition-" + this.partitionID);
        this.messagesDataFile = new File(this.fileRoot + "/" + "messagesData");
        if(!fileRoot.exists()){
            fileRoot.mkdirs();
            
        }
        if(!messagesDataFile.exists()){
            messagesDataFile.mkdirs();
        }
        
        this.incomingQueues =new BDBMap<String, String>(
                job,
                messagesDataFile, 
                "incomingQueues",
                String.class,
                String.class); 
        this.incomedQueues =new BDBMap<String, String>(
                job,
                messagesDataFile, 
                "incomedQueues",
                String.class,
                String.class);
    }
    @Override
    public void clearAllQueues() {
        clearIncomedQueues();
        clearIncomingQueues();
        clearOutgoingQueues();
        this.incomingQueues.shutdown();
        this.incomedQueues.shutdown();
        deleteFile(this.messagesDataFile.toString());
    }

    @Override
    public void clearIncomedQueues() {
        this.incomedQueues.clear();
    }

    @Override
    public void clearIncomingQueues() {
        this.incomingQueues.clear();
    }

    @Override
    public void clearOutgoingQueues() {
        this.outgoingQueues.clear();
    }

    @Override
    public void exchangeIncomeQueues() {

        clearIncomedQueues();              
        BDBMap<String, String> tempQueues = this.incomedQueues;    
        this.incomedQueues = this.incomingQueues;
        this.incomingQueues = tempQueues;

    }

    @Override
    public int getIncomedQueuesSize() {

        return incomedQueues.size();
    }

    @Override
    public int getIncomingQueuesSize() {
        
        return incomingQueues.size();
    }

    @Override
    public String getMaxIncomingQueueIndex() {
        return null;
    }

    @Override
    public String getMaxOutgoingQueueIndex() {
        int maxSize = 0;
        String maxIndex = null;
        ConcurrentLinkedQueue<BSPMessage> tempQueue = null;
        Entry<String, ConcurrentLinkedQueue<BSPMessage>> entry = null;
        Iterator<Entry<String, ConcurrentLinkedQueue<BSPMessage>>> it = 
            this.outgoingQueues.entrySet().iterator();
        
        while (it.hasNext()) { // Get the queue with the max size

            entry = it.next();
            tempQueue = entry.getValue();

            if (tempQueue.size() > maxSize) {
                maxSize = tempQueue.size();
                maxIndex = entry.getKey();
            } // if

        } // while
        
        return maxIndex;
    }

    @Override
    public int getOutgoingQueuesSize() {
        
        int size = 0;

        Entry<String, ConcurrentLinkedQueue<BSPMessage>> entry;
        Iterator<Entry<String, ConcurrentLinkedQueue<BSPMessage>>> it = this.outgoingQueues
                .entrySet().iterator();
        ConcurrentLinkedQueue<BSPMessage> queue;

        while (it.hasNext()) {

            entry = it.next();

            queue = entry.getValue();

            size += queue.size();

        }

        return size;
    }

    @Override
    public void incomeAMessage(String dstVertexID, BSPMessage msg) {
        
        StringBuffer unique = new StringBuffer();
        
        unique.append(incomingQueues.size());
        unique.append("$");
        unique.append(msg.intoString());
        incomingQueues.put(dstVertexID, unique.toString());
    }


    @Override
    public void outgoAMessage(String outgoingIndex, BSPMessage msg) {
        ConcurrentLinkedQueue<BSPMessage> outgoingQueue = outgoingQueues.get(outgoingIndex);

        if (outgoingQueue == null) {
            outgoingQueue = new ConcurrentLinkedQueue<BSPMessage>();
        }
        outgoingQueue.add(msg);
        outgoingQueues.put(outgoingIndex, outgoingQueue);
    }

    @Override
    public ConcurrentLinkedQueue<BSPMessage> removeIncomedQueue(String dstVertexID) {
        

        ConcurrentLinkedQueue<BSPMessage> incomedQueue = null;

        if(incomedQueues.containsKey(dstVertexID)){
            incomedQueue  = incomedQueues.getDupilcates(dstVertexID);      
            incomedQueues.delete(dstVertexID);
        }
        else{
            incomedQueue = new ConcurrentLinkedQueue<BSPMessage>();
        }

        return incomedQueue;
    }

    @Override
    public int getIncomingQueueSize(String dstVertexID) {
        
        if (incomingQueues.containsKey(dstVertexID)) {           
            return incomingQueues.getkeyFrequnce(dstVertexID);
        } else {
            return 0;
        }
    }
    
    @Override
    public ConcurrentLinkedQueue<BSPMessage> removeIncomingQueue(String dstVertexID) {
        
        ConcurrentLinkedQueue<BSPMessage> incomingQueue = null;

        if(incomingQueues.containsKey(dstVertexID)){
            incomingQueue  = incomingQueues.getDupilcates(dstVertexID);   
        }
        else{
            incomingQueue = new ConcurrentLinkedQueue<BSPMessage>();
        }

        return incomingQueue;
    }

    @Override
    public ConcurrentLinkedQueue<BSPMessage> removeOutgoingQueue(String index) {
                      

        ConcurrentLinkedQueue<BSPMessage> outgoingQueue = outgoingQueues.remove(index);
        
        if (outgoingQueue == null) {
            outgoingQueue = new ConcurrentLinkedQueue<BSPMessage>();
        }
        return outgoingQueue;
    }

    @Override
    public int getOutgoingQueueSize(String index) {
        
        if (outgoingQueues.containsKey(index)) {
            return outgoingQueues.get(index).size();
        } else {
            return 0;
        }
    }
    
    private void deleteFile(String filepath){ 

        File f = new File(filepath);
        if (f.exists() && f.isDirectory()) {
            if (f.listFiles().length==0) {
                f.delete();
            } else {
                File delFile[] = f.listFiles();
                int i = f.listFiles().length;
                for (int j = 0; j < i; j ++) {
                    if (delFile[j].isDirectory()) {
                        deleteFile(delFile[j].getAbsolutePath());
                    }
                    delFile[j].delete();
                }
            }
        }
     }




    @Override
    public void showMemoryInfo() {
    }
    @Override
    public String getNextOutgoingQueueIndex() {
        
        String nextIndex = null;
        
        synchronized(this.outgoingQueues) {
            
            int size = this.outgoingQueues.size();
            
            if (size == 0) {
                return null;
            }
            
            if (this.nextOutgoingQueueCount >= size) {
                this.nextOutgoingQueueCount = 1;
            }
            
            Entry<String, ConcurrentLinkedQueue<BSPMessage>> entry = null;
            Iterator<Entry<String, ConcurrentLinkedQueue<BSPMessage>>> it = this.outgoingQueues.entrySet().iterator();
            
            for (int i = 0; i < this.nextOutgoingQueueCount; i ++) {
                entry = it.next();
                nextIndex = entry.getKey();
            }
            this.nextOutgoingQueueCount ++;
        }
        
        return nextIndex;
    }


}