/**
 * CopyRight by Chinamobile
 * 
 * AggregatorInterface.java
 */
package com.chinamobile.bcbsp.api;

/**
 * Interface Aggregator for user to define.
 * 
 * @author
 * @version
 */
public interface AggregatorInterface<T> {
    /**
     * The method for aggregate algorithm that should be implements by the user.
     * 
     * @param aggValues
     * @return the aggregate result
     */
    abstract public T aggregate(Iterable<T> aggValues);
}
