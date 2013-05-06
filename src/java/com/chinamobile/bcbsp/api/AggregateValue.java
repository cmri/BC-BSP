/**
 * CopyRight by Chinamobile
 * 
 * AggregateValue.java
 */
package com.chinamobile.bcbsp.api;

import org.apache.hadoop.io.Writable;

import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;

/**
 * AggregateValue
 * 
 * Abstract class that implements AggregateValueInterface, it should be extended
 * by the user to define own aggregate value data structure.
 * 
 * @author
 * @version
 */
public abstract class AggregateValue<T> implements AggregateValueInterface<T>,
        Writable, Cloneable {

    /**
     * The default implementation of initBeforeSuperStep does nothing.
     * 
     * @param context
     */
    @Override
    public void initBeforeSuperStep(SuperStepContextInterface context) {
        
    }
    
    @SuppressWarnings("unchecked")
    public Object clone() {
        AggregateValue<T> aggValue = null;
        try {
            aggValue = (AggregateValue<T>) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return aggValue;
    }
}
