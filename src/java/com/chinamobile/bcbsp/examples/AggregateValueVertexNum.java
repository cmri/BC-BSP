/**
 * CopyRight by Chinamobile
 * 
 * AggregateValueVertexNum.java
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
 * AggregateValueVertexNum
 * An example implementation of AggregateValue.
 * 
 * @author Bai Qiushi
 * @version 1.0 2012-6-1
 *
 */
public class AggregateValueVertexNum extends AggregateValue<Long> {

    Long vertexNum;
    
    @Override
    public Long getValue() {
        return this.vertexNum;
    }

    @Override
    public void initValue(String s) {
        this.vertexNum = Long.valueOf(s);
    }

    @Override
    public void initValue(Iterator<BSPMessage> messages,
            AggregationContextInterface context) {
        this.vertexNum = 1L;
    }

    @Override
    public void setValue(Long value) {
        this.vertexNum = value;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.vertexNum = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.vertexNum);
    }

    @Override
    public String toString() {
        return String.valueOf(this.vertexNum);
    }
}
