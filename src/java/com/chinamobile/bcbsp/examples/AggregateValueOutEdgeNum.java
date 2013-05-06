/**
 * CopyRight by Chinamobile
 * 
 * AggregateValueOutEdgeNum.java
 */
package com.chinamobile.bcbsp.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.AggregationContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;

/**
 * AggregateValueOutEdgeNum
 * An example implementation of AggregateValue.
 * 
 * @author Bai Qiushi
 * @version 1.0 2011-12-08
 */
public class AggregateValueOutEdgeNum extends AggregateValue<Long>{
    
    Long outgoingEdgeNum;
    
    public Long getValue(){
        return this.outgoingEdgeNum;
    }
    
    public void setValue(Long value){
        this.outgoingEdgeNum = value;
    }
    /**
     * Constructor
     */
    public AggregateValueOutEdgeNum(){
    }
    
    public String toString() {
        return String.valueOf(this.outgoingEdgeNum);
    }

    @Override
    public void initValue(String s) {
        this.outgoingEdgeNum = Long.valueOf(s);
    }

    @Override
    public void initValue(Iterator<BSPMessage> messages, AggregationContextInterface context) {
        
        this.outgoingEdgeNum = (long) context.getOutgoingEdgesNum();
        
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        
        this.outgoingEdgeNum = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeLong(this.outgoingEdgeNum);
    }
}
