/**
 * CopyRight by Chinamobile
 * 
 * SumOutEdgeNumAggregator.java
 */
package com.chinamobile.bcbsp.examples;

import com.chinamobile.bcbsp.api.Aggregator;

/**
 * SumOutEdgeNumAggregator
 * 
 * An example implementation of Aggregator.
 * To do the sum operation on out edge number.
 * 
 * @author Bai Qiushi
 * @version 1.0 2012-6-1
 */
public class SumOutEdgeNumAggregator extends
        Aggregator<AggregateValueOutEdgeNum> {

    @Override
    public AggregateValueOutEdgeNum aggregate(
            Iterable<AggregateValueOutEdgeNum> aggValues) {

        long sum = 0;
        
        for (AggregateValueOutEdgeNum aggValue : aggValues) {
            
            sum = sum + aggValue.getValue();
        }
        
        AggregateValueOutEdgeNum result = new AggregateValueOutEdgeNum();
        
        result.setValue(sum);
        
        return result;
    }

}
