/**
 * ShortestPathCombiner.java
 */
package com.chinamobile.bcbsp.examples.sssp;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.api.Combiner;
import com.chinamobile.bcbsp.comm.BSPMessage;

/**
 * ShortestPathCombiner An example implementation of combiner, to get the
 * shortest distance from the data of all messages sent to the same destination
 * vertex, and return only one message.
 * 
 * @author LiBingLiang
 * @version 0.1
 */
public class ShortestPathCombiner extends Combiner {
	public static final Log LOG = LogFactory.getLog(ShortestPathCombiner.class);
	private int shortestDis = 0;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.chinamobile.bcbsp.api.Combiner#combine(java.util.Iterator)
	 */
	@Override
	public BSPMessage combine(Iterator<BSPMessage> messages) {

		BSPMessage msg = new BSPMessage();
		shortestDis = SSP_BSP.MAXIMUM;
		while (messages.hasNext()) {

			msg = messages.next();
			int tmpValue = Integer.parseInt(new String(msg.getData()));
			if (shortestDis > tmpValue) {
				shortestDis = tmpValue;
			}
		}

		String newData = Integer.toString(shortestDis);
		msg = new BSPMessage(msg.getDstPartition(), msg.getDstVertexID(),
				newData.getBytes());
		return msg;
	}

}
