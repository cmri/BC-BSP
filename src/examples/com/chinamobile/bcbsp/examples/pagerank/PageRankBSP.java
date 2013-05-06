package com.chinamobile.bcbsp.examples.pagerank;
/**
 * PageRankBSP.java
 */
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContextInterface;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;

/**
 * PageRankBSP.java
 * This is the user-defined arithmetic which implements {@link BSP}.
 * 
 * @author WangZhigang
 * @version 0.1 2012-2-17
 */

public class PageRankBSP extends BSP {
	
	public static final Log LOG = LogFactory.getLog(PageRankBSP.class);
	public static final String ERROR_SUM = "aggregator.error.sum";
	public static final double ERROR_THRESHOLD = 0.1;
	public static final double CLICK_RP = 0.0001;
	public static final double FACTOR = 0.15;

	// Variables of the Graph.
	private float newVertexValue = 0.0f;
	private float receivedMsgValue = 0.0f;
	private float receivedMsgSum = 0.0f;
	private float sendMsgValue = 0.0f;
	@SuppressWarnings("unchecked")
	private Iterator<Edge> outgoingEdges;
	private PREdgeLite edge;
	private BSPMessage msg;
	private ErrorAggregateValue errorValue;
	
	private int failCounter = 0;
	
	@Override
	public void setup(Staff staff) {
		this.failCounter = staff.getFailCounter();
		LOG.info("Test FailCounter: " + this.failCounter);
	}
	
	@Override
	public void compute(Iterator<BSPMessage> messages, BSPStaffContextInterface context) 
			throws Exception {				
		// Receive messages sent to this Vertex.
		receivedMsgValue = 0.0f;
		receivedMsgSum = 0.0f;
		while (messages.hasNext()) {
			receivedMsgValue = Float.parseFloat(new String(messages.next().getData()));
			receivedMsgSum += receivedMsgValue;
		}
		
		PRVertexLite vertex = (PRVertexLite) context.getVertex();
		
		// Just for creating a fault manually.
		if (context.getCurrentSuperStepCounter() == 7 && vertex.getVertexID() == 3858241 && this.failCounter < 3) {
			LOG.error("Fault manually : <VertexID>" + vertex.getVertexID());
			throw new Exception(vertex.getVertexID().toString());
		}
		
		// Handle received messages and Update vertex value.
		if (context.getCurrentSuperStepCounter() == 0) {
			sendMsgValue = Float.valueOf(vertex.getVertexValue())/context.getOutgoingEdgesNum(); // old vertex value
		} else {
			// According to the sum of error to judge the convergence.
			errorValue = (ErrorAggregateValue)context.getAggregateValue(ERROR_SUM);
			if (Double.parseDouble(errorValue.getValue()) < ERROR_THRESHOLD) {
				context.voltToHalt();
				return;
			}
			
			newVertexValue = (float) (CLICK_RP * FACTOR + receivedMsgSum * (1 - FACTOR));
			sendMsgValue = newVertexValue / context.getOutgoingEdgesNum();
			vertex.setVertexValue(newVertexValue);
			context.updateVertex(vertex);
		}
		
		// Send new messages.
		outgoingEdges = context.getOutgoingEdges();
		while (outgoingEdges.hasNext()) {
			edge = (PREdgeLite) outgoingEdges.next();
			msg = new BSPMessage(String.valueOf(edge.getVertexID()),
					Float.toString(sendMsgValue).getBytes());
			context.send(msg);
		}
		
		return;
	  }

	@Override
	public void initBeforeSuperStep(SuperStepContextInterface arg0) {
	}
}
