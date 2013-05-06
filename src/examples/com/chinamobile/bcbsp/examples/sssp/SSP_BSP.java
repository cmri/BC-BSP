package com.chinamobile.bcbsp.examples.sssp;

/**
 * ShortestPath.java
 */
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContextInterface;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.util.BSPJob;

/**
 * ShortestPath.java This is the user-defined arithmetic which implements
 * {@link BSP}.
 * 
 * @author LiBingLiang
 * @version 0.1 2012-3-6
 */

public class SSP_BSP extends BSP {

	public static final Log LOG = LogFactory.getLog(SSP_BSP.class);

	// Variables of the Graph.
	public final static String SOURCEID = "bcbsp.sssp.source.id";
	public int sourceID = 1;
	
	private int sendMsgValue = 0;
	@SuppressWarnings("unchecked")
	private Iterator<Edge> AdjacentNodes;
	private SSPEdge edge=null;
	private BSPMessage msg;
	public static final int MAXIMUM = Integer.MAX_VALUE / 2;
	private int newVertexValue;

	private int newDistance = MAXIMUM;
	private int eachNodeDistance = 0;
	
	@Override
	public void initBeforeSuperStep(SuperStepContextInterface context) {
		BSPJob job = context.getJobConf();
		if (context.getCurrentSuperStepCounter() == 0) {
			this.sourceID = job.getInt(SOURCEID, 1);
		}
	}
	
	@Override
	public void compute(Iterator<BSPMessage> messages,
			BSPStaffContextInterface context) throws Exception {

		SSPVertex vertex =(SSPVertex) context.getVertex();
		if (context.getCurrentSuperStepCounter() == 0) {

			int vertexID = vertex.getVertexID();
			int dis;

			if (vertexID == sourceID) {		
				LOG.info("Source:" + vertexID);
				dis = 0;
				notityAdjacentNode(0, context);
			} else {
				dis = MAXIMUM;
			}
			newVertexValue = dis;
			vertex.setVertexValue(newVertexValue);
			context.updateVertex(vertex);
		}// superstep == 0
		else {
			newDistance = getNewDistance(messages);
			int previousDis = vertex.getVertexValue();
			if (previousDis>newDistance) {
				notityAdjacentNode(newDistance, context);
				vertex.setVertexValue(newDistance);
				context.updateVertex(vertex);
			}
			

		}// else
		context.voltToHalt();
	}// compute

	private void notityAdjacentNode(int nodedis,
			BSPStaffContextInterface context) {

		AdjacentNodes = context.getOutgoingEdges();
		while (AdjacentNodes.hasNext()) {
			edge = (SSPEdge) AdjacentNodes.next();
			sendMsgValue = edge.getEdgeValue()	+ nodedis;
			msg = new BSPMessage(String.valueOf(edge.getVertexID()),
					Integer.toString(sendMsgValue).getBytes());
			context.send(msg);

		}
	}

	private int getNewDistance(Iterator<BSPMessage> messages) {
		int shortestDis = MAXIMUM;
		while (messages.hasNext()) {
			eachNodeDistance = new Integer(
					new String(messages.next().getData()));
			if (eachNodeDistance < shortestDis) {
				shortestDis = eachNodeDistance;
			}
		}
		return shortestDis;
	}

//	@Override
//	public void initBeforeSuperStep(SuperStepContextInterface arg0) {
//		// TODO Auto-generated method stub
//		
//	}
}