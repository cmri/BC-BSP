/**
 * SumCombiner.java
 */
package com.chinamobile.bcbsp.examples.pagerank;

import java.util.Iterator;

import com.chinamobile.bcbsp.api.Combiner;
import com.chinamobile.bcbsp.comm.BSPMessage;

/**
 * SumCombiner
 * An example implementation of combiner,
 * to sum the data of all messages sent
 * to the same destination vertex, and
 * return only one message.
 * 
 * @author Bai Qiushi
 * @version 0.1
 */
public class SumCombiner extends Combiner {

	/* (non-Javadoc)
	 * @see com.chinamobile.bcbsp.api.Combiner#combine(java.util.Iterator)
	 */
	@Override
	public BSPMessage combine(Iterator<BSPMessage> messages) {
		
		BSPMessage msg;
		double sum = 0.0;
		do {
			msg = messages.next();
			String tmpValue = new String(msg.getData());
			sum = sum + Double.parseDouble(tmpValue);
		} while (messages.hasNext());
		
		String newData = Double.toString(sum);
		
		msg = new BSPMessage(msg.getDstPartition(), msg.getDstVertexID(), newData.getBytes());
		
		return msg;
	}

}
