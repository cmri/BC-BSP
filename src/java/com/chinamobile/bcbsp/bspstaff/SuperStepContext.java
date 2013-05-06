/**
 * CopyRight by Chinamobile
 * 
 * SuperStepContext.java
 */
package com.chinamobile.bcbsp.bspstaff;

import java.util.HashMap;

import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.util.BSPJob;

/**
 * SuperStepContext
 * This class implements {@link SuperStepContextInterface}.
 * Methods defined in the SuperStepContextInterface can be used by users.
 * While other methods only can be invoked by {@link BSPStaff}.
 * 
 * @author Bai Qiushi
 * @version 1.0
 */
public class SuperStepContext implements SuperStepContextInterface {

    private BSPJob jobConf;
    private int currentSuperStepCounter;
    @SuppressWarnings("unchecked")
    private HashMap<String, AggregateValue> aggregateValues; // Aggregate values from the previous super step.
    
    
    @SuppressWarnings("unchecked")
    public SuperStepContext(BSPJob jobConf, int currentSuperStepCounter) {
        this.jobConf = jobConf;
        this.currentSuperStepCounter = currentSuperStepCounter;
        this.aggregateValues = new HashMap<String, AggregateValue>();
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

    @Override
    public int getCurrentSuperStepCounter() {
        return this.currentSuperStepCounter;
    }

    @Override
    public BSPJob getJobConf() {
        return this.jobConf;
    }

}
