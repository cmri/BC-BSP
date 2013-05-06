/**
 * CopyRight by Chinamobile
 * 
 * BSPStaffContextInterface.java
 */
package com.chinamobile.bcbsp.bspstaff;

import java.util.Iterator;

import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.util.BSPJob;

/**
 * BSPStaffContextInterface.java
 * This is a context for prcessing one vertex.
 * All methods defined in this interface can be used by users in {@link BSP}.
 * 
 * @author WangZhigang, changed by Bai Qiushi
 * @version 1.0
 */

public interface BSPStaffContextInterface {
    
    /**
     * Get the vertex.
     * 
     * @return
     */
    @SuppressWarnings("unchecked")
    public Vertex getVertex();
    
    /**
     * Update the vertex.
     * 
     * @param vertex
     */
    @SuppressWarnings("unchecked")
    public void updateVertex(Vertex vertex);
    
    /**
     * Get the number of outgoing edges.
     * 
     * @return
     */
    public int getOutgoingEdgesNum();
    
    /**
     * Get outgoing edges.
     * 
     * @return
     */
    @SuppressWarnings("unchecked")
    public Iterator<Edge> getOutgoingEdges();
    
    /**
     * Remove one outgoing edge.
     * If the edge does not exist, return false.
     * 
     * @param edge
     * @return <code>true</code> if the operation is successfull, else <code>false</code>.
     */
    @SuppressWarnings("unchecked")
    public boolean removeEdge(Edge edge);
    
    /**
     * Update one outgoing edge.
     * If the edge exists, update it, else add it.
     * 
     * @param edge
     * @return <code>true</code> if the operation is successfull, else <code>false</code>.
     */
    @SuppressWarnings("unchecked")
    public boolean updateEdge(Edge edge);
    
    /**
     * Get the current superstep counter.
     * 
     * @return
     */
    public int getCurrentSuperStepCounter();
    
    /**
     * Get the BSP Job Configuration.
     * 
     * @return
     */
    public BSPJob getJobConf();
    
    /**
     * User interface to send a message in the compute function.
     * 
     * @param msg
     */
    public void send(BSPMessage msg);
    
    /**
     * User interface to get an aggregate value aggregated from the previous super step.
     * 
     * @param name
     * @return
     */
    @SuppressWarnings("unchecked")
    public AggregateValue getAggregateValue(String name);
    
    /**
     * User interface to set current head node into inactive state.
     */
    public void voltToHalt();
}
