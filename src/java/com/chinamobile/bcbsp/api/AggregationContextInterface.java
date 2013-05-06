/**
 * CopyRight by Chinamobile
 * 
 * AggregationContextInterface.java
 */
package com.chinamobile.bcbsp.api;

import java.util.Iterator;
import com.chinamobile.bcbsp.util.BSPJob;

/**
 * @author Bai Qiushi
 * @version 0.1
 */
public interface AggregationContextInterface {

    /**
     * Get the VertexID.
     * 
     * @return
     */
    public String getVertexID();
    
    /**
     * Get the value of one vertex.
     * 
     * @return
     */
    public String getVertexValue();
    
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
    public Iterator<Edge<?,?>> getOutgoingEdges();
    
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
     * User interface to get an aggregate value aggregated from the previous super step.
     * 
     * @param name
     * @return
     */
    @SuppressWarnings("unchecked")
    public AggregateValue getAggregateValue(String name);
}
