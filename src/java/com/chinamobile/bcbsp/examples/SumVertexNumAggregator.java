/**
 * CopyRight by Chinamobile
 * 
 * SumVertexNumAggregator.java
 */
package com.chinamobile.bcbsp.examples;

import com.chinamobile.bcbsp.api.Aggregator;

/**
 * SumVertexNumAggregator
 * 
 * An example implementation of Aggregator.
 * To do the sum operation on vertex numbers.
 * 
 * @author Bai Qiushi
 * @version 1.0 2012-6-1
 */
public class SumVertexNumAggregator extends Aggregator<AggregateValueVertexNum> {

    @Override
    public AggregateValueVertexNum aggregate(
            Iterable<AggregateValueVertexNum> aggValues) {
        
        long sum = 0L;
        
        for (AggregateValueVertexNum aggValue : aggValues) {
            
            sum = sum + aggValue.getValue();
        }
        
        AggregateValueVertexNum result = new AggregateValueVertexNum();
        
        result.setValue(sum);
        
        return result;
    }

}
