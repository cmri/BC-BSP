/**
 * CopyRight by Chinamobile
 * 
 * MaxAggregator.java
 */
package com.chinamobile.bcbsp.examples;

import com.chinamobile.bcbsp.api.Aggregator;

/**
 * MaxAggregator
 * 
 * An example implementation of Aggregator.
 * To do the max operation on outgoing edge number.
 * 
 * @author Bai Qiushi
 * @version 1.0 2011-12-08
 */
public class MaxAggregator extends Aggregator<AggregateValueOutEdgeNum> {

    /** Implemented by the user */
    @Override
    public AggregateValueOutEdgeNum aggregate(Iterable<AggregateValueOutEdgeNum> aggValues) {
        
        long max = 0;
        
        for (AggregateValueOutEdgeNum aggValue : aggValues) {
            if (aggValue.getValue() > max) {
                max = aggValue.getValue();
            }
        }
        
        AggregateValueOutEdgeNum result = new AggregateValueOutEdgeNum();
        
        result.setValue(max);
        
        return result;
    }// end-aggregate
}
