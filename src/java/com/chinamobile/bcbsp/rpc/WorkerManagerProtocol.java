/**
 * CopyRight by Chinamobile
 * 
 * WorkerManagerProtocol.java
 */
package com.chinamobile.bcbsp.rpc;

import java.io.IOException;

import com.chinamobile.bcbsp.action.Directive;
import com.chinamobile.bcbsp.util.BSPJobID;

/**
 * WorkerManagerProtocol
 * 
 * A protocol for BSPController talks to WorkerManager. This protocol allow
 * BSPController dispatch tasks to a WorkerManager.
 * 
 * @author
 * @version
 */
public interface WorkerManagerProtocol extends BSPRPCProtocolVersion {

    /**
     * Instruct WorkerManager performaning tasks.
     * 
     * @param directive
     *            instructs a WorkerManager performing necessary execution.
     * @throws IOException
     */
    boolean dispatch(BSPJobID jobId, Directive directive, boolean isRecovery, boolean changeWorkerState, int failCounter) throws IOException;
    
    public void clearFailedJobList();
    public void addFailedJob(BSPJobID jobId);    
    public int getFailedJobCounter();
}