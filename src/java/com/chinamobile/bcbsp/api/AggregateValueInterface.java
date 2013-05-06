/**
 * CopyRight by Chinamobile
 * 
 * AggregateValueInterface.java
 */
package com.chinamobile.bcbsp.api;

import java.util.Iterator;

import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;

/**
 * AggregateValue Interface
 * 
 * @author
 * @version
 */
public interface AggregateValueInterface<T> {

    public T getValue();

    public void setValue(T value);

    /**
     * For the value to transferred through the synchronization process.
     * 
     * @return String
     */
    public String toString();

    /**
     * For the value to transferred through the synchronization process.
     * 
     * @param s
     *            String
     */
    public void initValue(String s);

    /**
     * A user defined method that will be called for each vertex of the graph to
     * init the aggregate value from the information of the graph.
     * 
     * @param Iterator<BSPMessage> messages
     * @param AggregationContextInterface context
     */
    public void initValue(Iterator<BSPMessage> messages, AggregationContextInterface context);
    
    /**
     * Initialize before each super step.
     * User can init some global variables for each super step.
     * 
     * @param context SuperStepContextInterface
     */
    public void initBeforeSuperStep(SuperStepContextInterface context);
}
