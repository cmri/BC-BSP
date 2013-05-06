/**
 * KMeansBSP.java
 */
package com.chinamobile.bcbsp.examples.kmeans;

import java.util.ArrayList;
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
 * KMeansBSP
 * This is the user-defined arithmetic which implements {@link BSP}.
 * Implements the basic k-means algorithm.
 * 
 * @author Bai Qiushi
 * @version 0.2 2012-2-28
 */
public class KMeansBSP extends BSP {

	public static final Log LOG = LogFactory.getLog(KMeansBSP.class);
	public static final String KMEANS_K = "kmeans.k";
	public static final String KMEANS_CENTERS = "kmeans.centers";
	public static final String AGGREGATE_KCENTERS = "aggregate.kcenters";
	
	private BSPJob jobconf;
	private int superStepCount;
	private int K;
	private int dimension;
	private ArrayList<ArrayList<Float>> kCenters = new ArrayList<ArrayList<Float>>();
	
	//The threshold for average error between the new k centers and the last k centers.
	private final double errors_Threshold = 0.01;
	//The real average error between the new k centers and the last k centers.
	private double errors = Double.MAX_VALUE;
	
	@SuppressWarnings("unchecked")
	@Override
	public void compute(Iterator<BSPMessage> messages, BSPStaffContextInterface context)
			throws Exception {
		
		jobconf = context.getJobConf();
		superStepCount = context.getCurrentSuperStepCounter();
		ArrayList<Float> thisPoint = new ArrayList<Float>();
		KMVertex thisVertex = (KMVertex) context.getVertex();
		Iterator<Edge> outgoingEdges = context.getOutgoingEdges();
		//Init this point
		while (outgoingEdges.hasNext()) {
			KMEdge edge = (KMEdge) outgoingEdges.next();
			thisPoint.add( Float.valueOf(edge.getEdgeValue()) );
		}
			
		//Calculate the class tag of this vertex.
		byte tag = 0;
		double minDistance = Double.MAX_VALUE;
		//Find the shortest distance of this point with the kCenters.
		for (byte i = 0; i < kCenters.size(); i ++) {
			ArrayList<Float> center = kCenters.get(i);
			double dist = distanceOf(thisPoint, center);
			if (dist < minDistance) {
				tag = i;
				minDistance = dist;
			}
		}
		//Write the vertex's class tag into the vertex value.
		thisVertex.setVertexValue(tag);
		context.updateVertex(thisVertex);

		if (this.errors < this.errors_Threshold) {
			context.voltToHalt();
		}

	}//end-compute
	
	private double distanceOf(ArrayList<Float> p1, ArrayList<Float> p2) {
		double dist = 0.0;
		
		// dist = (x1-y1)^2 + (x2-y2)^2 + ... + (xn-yn)^2
		for (int i = 0; i < p1.size(); i ++) {
			dist = dist + (p1.get(i) - p2.get(i)) * (p1.get(i) - p2.get(i));
		}
		
		dist = Math.sqrt(dist);
		
		return dist;
	}

	@Override
	public void initBeforeSuperStep(SuperStepContextInterface context) {
		
		this.superStepCount = context.getCurrentSuperStepCounter();
		jobconf = context.getJobConf();
		if (superStepCount == 0) {
			this.K = Integer.valueOf(jobconf.get(KMeansBSP.KMEANS_K));
			//Init the k original centers from job conf.
			String originalCenters = jobconf.get(KMeansBSP.KMEANS_CENTERS);
			String[] centers = originalCenters.split("\\|");
			for (int i = 0; i < centers.length; i ++) {
				ArrayList<Float> center = new ArrayList<Float>();
				String[] values = centers[i].split("-");
				for (int j = 0; j < values.length; j ++) {
					center.add(Float.valueOf(values[j]));
				}
				kCenters.add(center);
			}
			this.dimension = kCenters.get(0).size();
			LOG.info("[KMeansBSP] K = " + K);
			LOG.info("[KMeansBSP] dimension = " + dimension);
			LOG.info("[KMeansBSP] k centers: ");
			for (int i = 0; i < K; i ++) {
				String tmpCenter = "";
				for (int j = 0; j < dimension; j ++) {
					tmpCenter = tmpCenter + " " + kCenters.get(i).get(j);
				}
				LOG.info("[KMeansBSP] <" + tmpCenter + " >");
			}
		}
		else {
			KCentersAggregateValue kCentersAgg = (KCentersAggregateValue) context.getAggregateValue(KMeansBSP.AGGREGATE_KCENTERS);
			ArrayList<ArrayList<Float>> newKCenters = new ArrayList<ArrayList<Float>>();
			
			//Calculate the new k centers and save them to newKCenters.
			ArrayList<ArrayList<Float>> contents = kCentersAgg.getValue();
			ArrayList<Float> nums = contents.get(K);
			for (int i = 0; i < K; i ++) {
				ArrayList<Float> center = new ArrayList<Float>();
				//Get the sum of coordinates of points in class i.
				ArrayList<Float> sum = contents.get(i);
				//Get the number of points in class i.
				float num = nums.get(i);
				for (int j = 0; j < dimension; j ++) {
					//the center's coordinate value.
					center.add(sum.get(j)/num);
				}
				//The i center.
				newKCenters.add(center);
			}
			
			this.errors = 0.0;
			//Calculate the errors sum between the new k centers and the last k centers.
			for (int i = 0; i < K; i ++) {
				for (int j = 0; j < dimension; j ++) {
					this.errors = this.errors + Math.abs(kCenters.get(i).get(j) - newKCenters.get(i).get(j));
				}
			}
			this.errors = this.errors / (K * dimension);
			
			this.kCenters.clear();
			this.kCenters = newKCenters;
			LOG.info("[KMeansBSP] k centers: ");
			for (int i = 0; i < K; i ++) {
				String tmpCenter = "[" + nums.get(i) + "]";
				for (int j = 0; j < dimension; j ++) {
					tmpCenter = tmpCenter + " " + kCenters.get(i).get(j);
				}
				LOG.info("[KMeansBSP] <" + tmpCenter + " >");
			}
		}
		
		LOG.info("[KMeansBSP]******* Error = " + errors + " ********");
	}

}
