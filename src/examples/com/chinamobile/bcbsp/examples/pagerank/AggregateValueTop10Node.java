/**
 * AggregateValueTop10Node.java
 */
package com.chinamobile.bcbsp.examples.pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.AggregationContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;

/**
 * AggregateValueTop10Node
 * An example implementation of AggregateValue.
 * To get the top 10 pagerank node.
 * 
 * @author Bai Qiushi
 * @version 0.1 2011-12-14
 */
public class AggregateValueTop10Node extends AggregateValue<String> {

    /**
     * In the form as follows:
     * id1=pr1|id2=pr2|...|id10=pr10
     */
    private String topTenNode;
    
    @Override
    public String getValue() {
        return topTenNode;
    }

    @Override
    public void initValue(String s) {
        this.topTenNode = s;
    }
    
    @Override
    public String toString() {
        return this.topTenNode;
    }

    @Override
    public void initValue(Iterator<BSPMessage> messages, AggregationContextInterface context) {
        
        String id = context.getVertexID();
        String pr = context.getVertexValue();
        this.topTenNode = id + "=" + pr;
    }

    @Override
    public void setValue(String value) {
        
        this.topTenNode = value;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        
        this.topTenNode = in.readLine();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        
        out.writeBytes(this.topTenNode);
    }

}
