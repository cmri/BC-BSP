/**
 * CopyRight by Chinamobile
 * 
 * AggregationContext.java
 */
package com.chinamobile.bcbsp.bspstaff;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.AggregationContextInterface;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.util.BSPJob;

/**
 * Aggregation context.
 * 
 * @author Bai Qiushi
 * @version 1.0
 */
public class AggregationContext implements AggregationContextInterface {

    private BSPJob jobConf;
    private Vertex<?,?,? extends Edge<?,?>> vertex;
    private Iterator<Edge<?,?>> outgoingEdgesClone;
    private int currentSuperStepCounter;
    @SuppressWarnings("unchecked")
    private HashMap<String, AggregateValue> aggregateValues; // Aggregate values from the previous super step.
    
    @SuppressWarnings("unchecked")
    public AggregationContext(BSPJob jobConf, Vertex<?,?,? 
            extends Edge<?,?>> aVertex, int currentSuperStepCounter) {
        this.jobConf = jobConf;
        this.vertex = aVertex;
        this.outgoingEdgesClone = new ArrayList<Edge<?,?>>(this.vertex.getAllEdges()).iterator();
        this.currentSuperStepCounter = currentSuperStepCounter;
        this.aggregateValues = new HashMap<String, AggregateValue>();
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
    public Iterator<Edge<?,?>> getOutgoingEdges() {
        return this.outgoingEdgesClone;
    }

    @Override
    public int getOutgoingEdgesNum() {
        return this.vertex.getEdgesNum();
    }

    @Override
    public String getVertexID() {
        return this.vertex.getVertexID().toString();
    }

    @Override
    public String getVertexValue() {
        return this.vertex.getVertexValue().toString();
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
    
    @SuppressWarnings("unchecked")
    @Override
    public AggregateValue getAggregateValue(String name) {
        return this.aggregateValues.get(name);
    }

}
