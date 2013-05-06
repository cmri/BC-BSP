/**
 * CopyRight by Chinamobile
 * 
 * BDBMap.java
 */
package com.chinamobile.bcbsp.comm;

import java.io.File;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import java.util.Map;
import java.util.Map.Entry;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.util.BSPJob;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredSortedMap;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseExistsException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.EnvironmentConfig;

/**
 * Persistent Map builded by BerkeleyDB Java edition uing je-4.10
 * 
 * @param <K>
 * @param <V>
 */
public class BDBMap<K extends Serializable, V extends Serializable> 
    implements Cloneable, Serializable{
    
    private static final Log LOG = LogFactory.getLog(BDBMap.class);
    
    private static final long serialVersionUID = 3427799316155220967L;

    private transient BdbEnvironment dbEnv;            // one database environment can contain several databases
    private transient Database mapDb;             // database for storing key-value
    private transient StoredSortedMap<K,V> storedMap;   // Stored Collections provided by dbd
    @SuppressWarnings("unused")
    private transient File dbDir;                 // directory  the database
    @SuppressWarnings("unused")
    private transient String dbName;                // database name
    private AtomicLong bdbMapSize = new AtomicLong(0);

    private Map<K,Integer> keyFrequnce = new HashMap<K,Integer>();


/**
 * Open an existing database 
 * @param db
 * @param keyClass
 * @param valueClass
 * @param classCatalog
 */
    public BDBMap(BSPJob job, Database db,Class<K> keyClass, Class<V> valueClass, StoredClassCatalog classCatalog){

        mapDb = db;
        dbName = db.getDatabaseName();
        bdbMapSize = new AtomicLong(0);
        bindDatabase(mapDb, keyClass, valueClass, classCatalog);
    }
/**
 * Create a database 
 * @param dbDir
 * @param dbName
 * @param keyClass
 * @param valueClass
 */
    public BDBMap(BSPJob job, File dbDir, String dbName, Class<K> keyClass, Class<V> valueClass){
        
        this.dbDir = dbDir;
        this.dbName = dbName;
        bdbMapSize = new AtomicLong(0);
        createAndBindDatabase(dbDir,dbName,keyClass,valueClass);
    }
        
    
/**
 * Bind a database db with BDB's Stored Collection
 * @param db
 * @param keyClass
 * @param valueClass
 * @param classCatalog
 */
    public void bindDatabase(Database db, Class<K> keyClass, Class<V> valueClass, StoredClassCatalog classCatalog){
        
        EntryBinding<K> keyBinding = TupleBinding.getPrimitiveBinding(keyClass);
        EntryBinding<V> valueBinding = TupleBinding.getPrimitiveBinding(valueClass);

        if(keyBinding == null) {
            keyBinding = new SerialBinding<K>(classCatalog, keyClass);
        }
        if(valueBinding == null) {
            valueBinding = new SerialBinding<V>(classCatalog, valueClass);
        }
        mapDb = db;
        storedMap = new StoredSortedMap<K,V>(
                db,                                             // db
                keyBinding,                                     //Key
                valueBinding,                                   // Value
                true); 

    }
/**
* Create and Bind a database db using bindDatabase()
* @param dbDir
* @param dbName
* @param keyClass
* @param valueClass
*/
    public void createAndBindDatabase(File dbDir, String dbName, Class<K> keyClass, Class<V> valueClass) throws DatabaseNotFoundException,
    DatabaseExistsException,DatabaseException,IllegalArgumentException{
        File envFile = null;
        EnvironmentConfig envConfig = null;
        DatabaseConfig dbConfig = null;
        Database db=null;

        try {
            
            envFile = dbDir;
            envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            envConfig.setTransactional(false);
            envConfig.setCachePercent(30);
            envConfig.setConfigParam("je.evictor.nodesPerScan","20");

            dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(false);
            dbConfig.setTemporary(true);
            dbConfig.setSortedDuplicates(true);
            

            dbEnv = new BdbEnvironment(envFile, envConfig);
            db = dbEnv.openDatabase(null, dbName, dbConfig);
            bindDatabase(db,keyClass,valueClass,dbEnv.getClassCatalog());
            
             
        } catch (DatabaseNotFoundException e) {
            throw e;
        } catch (DatabaseExistsException e) {
            throw e;
        } catch (DatabaseException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            throw e;
        }
        
    }

    /**
     * return current BDBMap size
     */
    public int size() {
        synchronized(bdbMapSize){            
                return (int)(bdbMapSize.get());               
        }
    }
    
    /**
     * 
     */
    public void keyFrequnceIncrement(K key){
        
        Integer freq = keyFrequnce.get(key);
     
        keyFrequnce.put(key, freq == null ? 1 : freq + 1);
    }
    

    
    public void keyFrequnceDecrement(K key, int count){
        
        Integer freq = keyFrequnce.get(key);    
        keyFrequnce.put(key, freq - count);
    }
    

    
    public int getkeyFrequnce(K key){
        
        int freq = 0;
        try {            
            freq =  keyFrequnce.get(key);
        } catch (Exception e) {
            LOG.error("BDB getkeyFrequnce Exception", e);
        }
        return freq;
        
    }
 

    public K getMostFrequetKey(){
        
        int maxSize = 0;
        Integer tmp;
        Entry<K, Integer> entry = null;
        Iterator<Entry<K, Integer>> it = 
                keyFrequnce.entrySet().iterator();
        K key = null;
        while (it.hasNext()) {
            
            entry = it.next();
            tmp = entry.getValue();

            if (tmp> maxSize) {
                maxSize = tmp;
                key = entry.getKey();
            } 

        } 

        return key;
    }
    
    /**
     * put a key-value pair into database
     */
    public V put(K key, V value) {                        
        synchronized(bdbMapSize){
           
            storedMap.put(key, value);   
            bdbMapSize.incrementAndGet();
        }
        return value;
        
    }
 
    /**
     * delete a key-value pair from database
     */
    public V delete(K key) {
        V value ;
        try {
            synchronized(bdbMapSize){
                if(storedMap.containsKey(key)){
                int count = getkeyFrequnce(key);
                value = storedMap.remove(key);  
                bdbMapSize.addAndGet(-count);
                }
                else value = null;
            }
            return value;
        } catch (Exception e) {
            LOG.error("BDB delete() Exception", e);
        }
        return null; 
    }
    
    public V get(K key) {
        V value = null;
        try {           
            value =  storedMap.get(key);
        } catch (Exception e) {
            LOG.error("BDB get() Exception", e);
        }
        return value;
        
    }
    /**
     * When duplicates are allowed, this method return all values associated with the key 
     */
    public ConcurrentLinkedQueue<BSPMessage> getDupilcates(K key){ 
        
        String tmp;
        int indexOf$;
        Collection<V> valueSet  = storedMap.duplicates(key);
        ConcurrentLinkedQueue<BSPMessage> list = new ConcurrentLinkedQueue<BSPMessage>();
        
        Iterator<V> it = valueSet.iterator();
        while(it.hasNext()){
            BSPMessage tmpmsg = new BSPMessage();
            tmp = it.next().toString();
            indexOf$ = tmp.indexOf('$');
            tmpmsg.fromString(tmp.substring(indexOf$+1));

            list.add(tmpmsg);
        }
        storedMap.remove(key);  
        bdbMapSize.addAndGet(-list.size());
        return list;   
        
    }

    /**
     * When duplicates are allowed, this method return  values' count associated with the key 
     */
    public int getDupilcatesCount(K key){   
         
        return storedMap.duplicates(key).size();   
        
    }
    
    /**
     * Return true if the database contains the key.Otherwise return false
     */
    public Boolean containsKey(K key){
        return storedMap.containsKey(key);
    }
    
    /**
     * Delete all key-value pairs in the database
     */
    public void clear(){
        keyFrequnce.clear();
        storedMap.clear();
        bdbMapSize.set(0);
      
    }
    
    public void shutdown(){
        mapDb.close();
        dbEnv.close();  
    }
}
