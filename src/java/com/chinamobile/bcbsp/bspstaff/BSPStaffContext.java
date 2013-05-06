/**
 * CopyRight by Chinamobile
 * 
 * BSPStaffContext.java
 */
package com.chinamobile.bcbsp.bspstaff;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.util.BSPJob;

/**
 * BSPStaffContext.java
 * This class implements {@link BSPStaffContextInterface}.
 * Methods defined in the BSPStaffContextInterface can be used by users.
 * While other methods only can be invoked by {@link BSPStaff}.
 * 
 * @author WangZhigang
 * @version 1.0
 */

public class BSPStaffContext implements BSPStaffContextInterface {

    private BSPJob jobConf;
    @SuppressWarnings("unchecked")
    private Vertex vertex;
    @SuppressWarnings("unchecked")
    private Iterator<Edge> outgoingEdgesClone;
    private int currentSuperStepCounter;
    private List<BSPMessage> messagesCache;
    @SuppressWarnings("unchecked")
    private HashMap<String, AggregateValue> aggregateValues; // Aggregate values from the previous super step.
    private boolean activeFlag; // Ture: the current vertex is active; False: is inactive.
    
    @SuppressWarnings("unchecked")
    public BSPStaffContext(BSPJob jobConf, Vertex aVertex, int currentSuperStepCounter) {
        this.jobConf = jobConf;
        this.vertex = aVertex;
        this.outgoingEdgesClone = new ArrayList<Edge>(this.vertex.getAllEdges()).iterator();
        this.currentSuperStepCounter = currentSuperStepCounter;
        
        this.messagesCache = new ArrayList<BSPMessage>();
        this.aggregateValues = new HashMap<String, AggregateValue>();
        this.activeFlag = true;
    }
    
    /**
     * Get the active state.
     * 
     * @return
     */
    public boolean getActiveFLag() {
        return this.activeFlag;
    }
    
    /**
     * Add an aggregate value (key-value).
     * 
     * @param String key
     * @param AggregateValue value
     */
    @SuppressWarnings("unchecked")
    public void addAggregateValues(String key, AggregateValue value) {
        this.aggregateValues.put(key, value);
    }
    
    /**
     * Get the iterator of messages in Cache.
     * 
     * @return
     */
    public Iterator<BSPMessage> getMessages() {
        return this.messagesCache.iterator();
    }
    
    /**
     * Cleanup the MessagesCache.
     * 
     * @return <code>true</code> if the operation is successfull, else <code>false</code>.
     */
    public boolean cleanMessagesCache() {
        boolean success = false;
        this.messagesCache.clear();
        success = true;
        
        return success;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Vertex getVertex() {
        return this.vertex;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void updateVertex(Vertex vertex) {
        this.vertex = vertex;
    }
    
    @Override
    public int getOutgoingEdgesNum() {
        return this.vertex.getEdgesNum();
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Edge> getOutgoingEdges() {
        return this.outgoingEdgesClone;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean removeEdge(Edge edge) {
        this.vertex.removeEdge(edge);
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean updateEdge(Edge edge) {
        this.vertex.updateEdge(edge);
        return true;
    }
    
    @Override
    public int getCurrentSuperStepCounter() {
        return this.currentSuperStepCounter;
    }
    
    @Override
    public BSPJob getJobConf() {
        return this.jobConf;
    }
    
    @Override
    public void send(BSPMessage msg) {
        this.messagesCache.add(msg);
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public AggregateValue getAggregateValue(String name) {
        return this.aggregateValues.get(name);
    }
    
    @Override
    public void voltToHalt() {
        this.activeFlag = false;
    }
}
