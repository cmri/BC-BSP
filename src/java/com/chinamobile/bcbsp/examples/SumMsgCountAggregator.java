/**
 * CopyRight by Chinamobile
 * 
 * SumMsgCountAggregator.java
 */
package com.chinamobile.bcbsp.examples;

import com.chinamobile.bcbsp.api.Aggregator;

/**
 * SumMsgCountAggregator
 * 
 * An example implementation of Aggregator.
 * To do the sum operation on messages count.
 * 
 * @author Bai Qiushi
 * @version 1.0 2011-12-08
 */
public class SumMsgCountAggregator extends Aggregator<AggregateValueMsgCount> {

    /** Implemented by the user */
    @Override
    public AggregateValueMsgCount aggregate(Iterable<AggregateValueMsgCount> aggValues) {
        
        long sum = 0;
        
        for (AggregateValueMsgCount aggValue : aggValues) {
            
            sum = sum + aggValue.getValue();
        }
        
        AggregateValueMsgCount result = new AggregateValueMsgCount();
        
        result.setValue(sum);
        
        return result;
    }// end-aggregate
}
