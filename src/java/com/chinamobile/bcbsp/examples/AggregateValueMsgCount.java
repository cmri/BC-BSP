/**
 * CopyRight by Chinamobile
 * 
 * AggregateValueMsgCount.java
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
 * AggregateValueMsgCount
 * An example implementation of AggregateValue.
 * 
 * @author Bai Qiushi
 * @version 1.0 2011-12-08
 */
public class AggregateValueMsgCount extends AggregateValue<Long> {

    Long msgCount;
    
    @Override
    public Long getValue() {
        
        return this.msgCount;
    }

    @Override
    public void setValue(Long value) {
        
        this.msgCount = value;
        
    }
    
    @Override
    public String toString() {
        return this.msgCount.toString();
    }

    @Override
    public void initValue(String s) {
        this.msgCount = Long.valueOf(s);
    }

    @Override
    public void initValue(Iterator<BSPMessage> messages, 
            AggregationContextInterface context) {
        
       long count = 0;      
        while (messages.hasNext()) {
            count ++;
            messages.next();
        }
        
        this.msgCount = count;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        
        this.msgCount = in.readLong();    
    }

    @Override
    public void write(DataOutput out) throws IOException {
        
        out.writeLong(this.msgCount);
    }

}
