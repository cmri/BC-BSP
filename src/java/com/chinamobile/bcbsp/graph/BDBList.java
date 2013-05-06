/**
 * CopyRight by Chinamobile
 * 
 * KeyComparator.java
 */
package com.chinamobile.bcbsp.graph;

import com.sleepycat.je.*;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Key comparator for DB keys
 */
@SuppressWarnings("unchecked")
class KeyComparator implements Comparator,Serializable {
    
    private static final long serialVersionUID = 1L;

    /**
     * Compares two DB keys.
     *
     * @param key1 first key
     * @param key2 second key
     *
     * @return comparison result
     */

    public int compare(Object key1, Object key2) {
        byte[] k1 = (byte[]) key1;
        byte[] k2 = (byte[]) key2;
        return new BigInteger(k1).compareTo(new BigInteger(k2));
    }



   

}
 
/**
 * Fast queue implementation on top of Berkley DB Java Edition.
 *
 
 * This class is thread-safe.
 */
public final class BDBList {
    
    public static final Log LOG = LogFactory.getLog(BDBList.class);
    
    @SuppressWarnings("unused")
    private final String dbEnvPath;
    /**
     * Berkley DB environment
     */
    private final Environment dbEnv;
 
    /**
     * Berkley DB instance for the queue
     */
    private  Database queueDatabase;
 
    /**
     * Queue cache size - number of element operations it is allowed to loose in case of system crash.
     */

 
    /**
     * This queue name.
     */
    private final String queueName;
 
    /**
     * Queue operation counter, which is used to sync the queue database to disk periodically.
     */
    @SuppressWarnings("unused")
    private int opsCounter;
 
    /**
     * Creates instance of persistent queue.
     *
     * @param dbEnvPath   queue database environment directory path
     * @param dbName      descriptive queue name
     * @param cacheSize      how often to sync the queue to disk
     */
    @SuppressWarnings("unchecked")
    public BDBList(final String dbEnvPath,
                 final String dbName) {
        this.dbEnvPath = dbEnvPath;
 
        // Setup database environment
        final EnvironmentConfig dbEnvConfig = new EnvironmentConfig();
        dbEnvConfig.setTransactional(false);
        dbEnvConfig.setAllowCreate(true);
        dbEnvConfig.setCachePercent(30);
        
        this.dbEnv = new Environment(new File(dbEnvPath),
                                  dbEnvConfig);
 
        // Setup non-transactional deferred-write queue database
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(false);
        dbConfig.setAllowCreate(true);
        //dbConfig.setDeferredWrite(true);
        dbConfig.setBtreeComparator(new KeyComparator());
        dbConfig.setTemporary(true);
        this.queueDatabase = dbEnv.openDatabase(null,
            dbName,
            dbConfig);
        this.queueName = dbName;

    }
 
    /**
     * Retrieves and returns element from the head of this queue.
     *
     * @return element from the head of the queue or null if queue is empty
     *
     * @throws IOException in case of disk IO failure
     */
    public String poll() throws IOException {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        final Cursor cursor = queueDatabase.openCursor(null, null);
        try {
            cursor.getFirst(key, data, LockMode.RMW);
            if (data.getData() == null)
                return null;
            final String res = new String(data.getData(), "UTF-8");
            cursor.delete();
            return res;
        } finally {
            cursor.close();
        }
    }
 
    /**
     * Pushes element to the tail of this queue.
     *
     * @param element element
     *
     * @throws IOException in case of disk IO failure
     */
    public synchronized void push(final String element) throws IOException {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Cursor cursor = queueDatabase.openCursor(null, null);
        try {
            cursor.getLast(key, data, LockMode.RMW);
           
            BigInteger prevKeyValue;
            if (key.getData() == null) {
                prevKeyValue = BigInteger.valueOf(-1);
            } else {
                prevKeyValue = new BigInteger(key.getData());
            }
            BigInteger newKeyValue = prevKeyValue.add(BigInteger.ONE);
 
            try {
                final DatabaseEntry newKey = new DatabaseEntry(
                        newKeyValue.toByteArray());
                final DatabaseEntry newData = new DatabaseEntry(
                        element.getBytes("UTF-8"));
                queueDatabase.put(null, newKey, newData);
            } catch (IOException e) {
                LOG.error("[push]", e);
            }
        } finally {
            cursor.close();
        }
    }
 
   /**
     * Returns the size of this queue.
     *
     * @return the size of the queue
     */
    public long size() {
        return queueDatabase.count();
    }
    
    public String getElement(int index){
        DatabaseEntry data = new DatabaseEntry();
        
        Cursor cursor = queueDatabase.openCursor(null, null);

        BigInteger indexKey = new BigInteger(String.valueOf(index));
        final DatabaseEntry findKey = new DatabaseEntry(
                indexKey.toByteArray());
        
        cursor.getSearchKey(findKey, data, LockMode.RMW);
        

        String res = null;
        try {
            res = new String(data.getData(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            LOG.error("[getElement]", e);
        }finally{
            cursor.close();
        }
        return res;

    }
    public void setEelment(int index, String Element){
        DatabaseEntry data = new DatabaseEntry();
        
        Cursor cursor = queueDatabase.openCursor(null, null);

        BigInteger indexKey = new BigInteger(String.valueOf(index));
        final DatabaseEntry findKey = new DatabaseEntry(
                indexKey.toByteArray());
        cursor.getSearchKey(findKey, data, LockMode.DEFAULT);
        DatabaseEntry replacementData;
        try {
            replacementData = new DatabaseEntry(Element.getBytes("UTF-8"));
            cursor.putCurrent(replacementData);
            
        } catch (UnsupportedEncodingException e) {
            LOG.error("[setElement]", e);
        } finally{
            cursor.close();
        }

    }
    public void deleteEelment(int index){
        DatabaseEntry data = new DatabaseEntry();
        
        Cursor cursor = queueDatabase.openCursor(null, null);

        BigInteger indexKey = new BigInteger(String.valueOf(index));
        final DatabaseEntry findKey = new DatabaseEntry(
                indexKey.toByteArray());
        cursor.getSearchKey(findKey, data, LockMode.RMW);
        cursor.delete();
    }
    
    
    public void clear()
            throws DatabaseException {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Cursor cursor = queueDatabase.openCursor(null, null);
            try {
                Boolean keysRenumbered = true;
                OperationStatus status = OperationStatus.SUCCESS;
                
                while (status == OperationStatus.SUCCESS) {
                    if (keysRenumbered) {
                        status = cursor.getFirst(key, data, LockMode.RMW);
                        keysRenumbered = false;
                    } else {
                        status = cursor.getNext(key, data, LockMode.RMW);                      
                        
                    if (status == OperationStatus.SUCCESS) {
                        cursor.delete();
                    }
                }
                }
            } finally {
                cursor.close();
            }
        }
    @SuppressWarnings("unchecked")
    public void clean(){
        try {  
            dbEnv.removeDatabase(null, queueName);
            
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(false);
            dbConfig.setAllowCreate(true);
            dbConfig.setBtreeComparator(new KeyComparator());
            dbConfig.setTemporary(true);
            this.queueDatabase = dbEnv.openDatabase(null,
                    queueName,
                dbConfig);
         } catch (DatabaseNotFoundException e) {
             LOG.error("[clean]", e);  
         } catch (DatabaseException e) {
             LOG.error("[clean]", e);
         }   
     }  
    
    public void close() {

        queueDatabase.close();
        dbEnv.close();
    }
    @SuppressWarnings("unused")
    private static void deleteFile(File file){ 
        if(file.exists()){                    
         if(file.isFile()){                    
          file.delete();                       
         }else if(file.isDirectory()){              
          File files[] = file.listFiles();               
          for(int i=0;i<files.length;i++){           
           deleteFile(files[i]);             
          } 
         } 
         file.delete(); 
        }
        else{ 
         //
        } 
     }
}
