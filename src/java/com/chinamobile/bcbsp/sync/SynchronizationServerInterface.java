/**
 * CopyRight by Chinamobile
 * 
 * SynchronizationServerInterface.java
 */
package com.chinamobile.bcbsp.sync;

/**
 * SynchronizationServerInterface
 * 
 * This is an interface which used for Synchronization Server.
 * It should be initialized when starting the BC-BSP cluster and
 * stopped when stopping the BC-BSP cluster.
 * 
 * @version 1.0
 */
public interface SynchronizationServerInterface {

    /**
     * Make a preparation and start the Synchronization Service.
     * 
     * @return
     */
    public boolean startServer();

    /**
     * Release relative resource and stop the Synchronization Service.
     * 
     * @return
     */
    public boolean stopServer();

}
