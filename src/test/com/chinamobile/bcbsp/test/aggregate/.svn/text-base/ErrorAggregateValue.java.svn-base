/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.aggregate;
/**
 * ErrorAggregateValue.java
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.AggregationContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;

/**
 * ErrorAggregateValue.java
 * This is the basic unit of ErrorSumAggregator.
 * 
 * @author WangZhigang
 * @version 0.1 2012-2-17
 */

public class ErrorAggregateValue extends AggregateValue<String> {
    
    private String errorValue;
    
    @Override
    public void initValue(Iterator<BSPMessage> messages, AggregationContextInterface context) {       
    	double oldVertexValue = Double.parseDouble(context.getVertexValue());
    	double newVertexValue = 0.0;
		double receivedMsgValue = 0.0;
		double receivedMsgSum = 0.0;

		while (messages.hasNext()) {
			receivedMsgValue = Double.parseDouble(new String(messages.next().getData()));
			receivedMsgSum += receivedMsgValue;
		}
		newVertexValue = 0.0001 * 0.15 + receivedMsgSum * 0.85;
		
		this.errorValue = Double.toString(Math.abs(newVertexValue - oldVertexValue));
    }
    
    @Override
    public void initValue(String s) {
        this.errorValue = s;
    }
    
    @Override
    public void setValue(String value) {       
        this.errorValue = value;
    }
    
    @Override
    public String getValue() {
        return this.errorValue;
    }
    
    @Override
    public String toString() {
        return this.errorValue;
    }

    @Override
    public void readFields(DataInput in) throws IOException {       
        this.errorValue = in.readLine();
    }

    @Override
    public void write(DataOutput out) throws IOException {     
        out.writeBytes(this.errorValue);
    }
}
