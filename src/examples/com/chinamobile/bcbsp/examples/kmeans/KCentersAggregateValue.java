/**
 * KCentersAggregateValue.java
 */
package com.chinamobile.bcbsp.examples.kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.AggregationContextInterface;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.util.BSPJob;

/**
 * KCentersAggregateValue
 * Contains the k classes' sum of coordinate values
 * and the number of points in each class.
 * 
 * @author Bai Qiushi
 * @version 0.1 2012-2-28
 */
public class KCentersAggregateValue extends
		AggregateValue<ArrayList<ArrayList<Float>>> {
	
	/**
	 * First k records are coordinates sum of k classes for current super step,
	 * last 1 record are numbers of k classes for current super step.
	 * As follows: (k+1) rows totally.
	 * --------------------
	 * <S11, S12, ..., S1n>
	 * <S21, S22, ..., S2n>
	 * ...
	 * <Sk1, Sk2, ..., Skn>
	 * --------------------
	 * <N1, N2, ..., Nk>
	 */
	private ArrayList<ArrayList<Float>> contents = new ArrayList<ArrayList<Float>>();
	
	private ArrayList<ArrayList<Float>> kCenters = new ArrayList<ArrayList<Float>>();
	private int K;
	private int dimension;
	
	/*public KCentersAggregateValue() {
		//Init the contents with all 0.0
		ArrayList<Float> nums = new ArrayList<Float>();
		for (int i = 0; i < K; i ++) {
			ArrayList<Float> sum = new ArrayList<Float>();
			for (int j = 0; j < dimension; j ++) {
				sum.add(0.0f);
			}
			contents.add(sum);
			nums.add(0.0f);
		}
		contents.add(nums);
	}*/
	
	@Override
	public ArrayList<ArrayList<Float>> getValue() {
		
		return contents;
	}

	/**
	 * In the form as follows:
	 * S11-S12-...-S1n|S21-S22-...-S2n|...|S2k1-S2k2-...S2kn|N1-N2-...-Nk
	 */
	@Override
	public void initValue(String arg0) {

		String[] centers = arg0.split("\\|");
		for (int i = 0; i < centers.length; i ++) {
			ArrayList<Float> center = new ArrayList<Float>();
			String[] values = centers[i].split("-");
			for (int j = 0; j < values.length; j ++) {
				center.add(Float.valueOf(values[j]));
			}
			contents.add(center);
		}
	}
	
	@Override
	public String toString() {
		
		String value = "";
		
		for (int i = 0; i < contents.size(); i ++) {
			
			ArrayList<Float> center = contents.get(i);
			
			if (i > 0) {
				value = value + "|";
			}
			
			for (int j = 0; j < center.size(); j ++) {
				
				if (j > 0) {
					value = value + "-";
				}
				
				value = value + center.get(j);
				
			}
		}
		
		return value;
	}

	@Override
	public void initValue(Iterator<BSPMessage> messages, AggregationContextInterface context) {
		
		//int superStepCount = context.getCurrentSuperStepCounter();
		
		//This point represented by the vertex.
		ArrayList<Float> thisPoint = new ArrayList<Float>();
		//Init this point
		Iterator<Edge<?,?>> bordersItr = context.getOutgoingEdges();
		KMEdge edge;
		while (bordersItr.hasNext()) {
			edge = (KMEdge) bordersItr.next();
			thisPoint.add( Float.valueOf(edge.getEdgeValue()) );
		}
		
		//Init the contents with all 0.0
		this.contents.clear();
		ArrayList<Float> nums = new ArrayList<Float>();
		for (int i = 0; i < K; i ++) {
			ArrayList<Float> sum = new ArrayList<Float>();
			for (int j = 0; j < dimension; j ++) {
				sum.add(0.0f);
			}
			contents.add(sum);
			nums.add(0.0f);
		}
		contents.add(nums);
		
		//Calculate the class tag of this vertex.
		int tag = 0;
		double minDistance = Float.MAX_VALUE;
		//Find the shortest distance of this point with the kCenters.
		for (int i = 0; i < kCenters.size(); i ++) {
			ArrayList<Float> center = kCenters.get(i);
			double dist = distanceOf(thisPoint, center);
			if (dist < minDistance) {
				tag = i;
				minDistance = dist;
			}
		}
		
		//Add the value of this vertex into the sum of tag class.
		ArrayList<Float> sum = contents.get(tag);
		for (int i = 0; i < dimension; i ++) {
			sum.set(i, thisPoint.get(i));
		}
		//Init the refered class's number to 1.
		nums = contents.get(K);
		nums.set(tag, 1.0f);
		
		/**
		 * If it's the first super step, the headnode has not been classified,
		 * so must init the sums and nums with original k centers given by the
		 * job configuration.
		 *//*
		if (superStepCount == 0) {
			
			ArrayList<ArrayList<Float>> kCenters = new ArrayList<ArrayList<Float>>();
			//The original k centers given.
			String Origincenters = job.get(KMeansBSP.KMEANS_CENTERS);
			//Init the k original centers.
			String centerStr;
			String valueStr;
			StringTokenizer centersStr = new StringTokenizer(Origincenters, "\\|");
			
			if (centersStr.countTokens() != K) {
				System.out.println("[InitAggregateValue] Centers input are less than K!");
				System.exit(-1);
			}
			
			while (centersStr.hasMoreTokens()) {
				
				ArrayList<Float> center = new ArrayList<Float>();
				
				centerStr = centersStr.nextToken();
				StringTokenizer valuesStr = new StringTokenizer(centerStr, "-");
				
				if (valuesStr.countTokens() != dimension) {
					System.out.println("[InitAggregateValue] Centers' values are less than dimension!");
					continue;
				}
				
				while (valuesStr.hasMoreTokens()) {
					valueStr = valuesStr.nextToken();
					center.add(Float.valueOf(valueStr));
				}
				kCenters.add(center);
			}
			
			//Calculate the class tag of this vertex.
			int tag = 0;
			double minDistance = Float.MAX_VALUE;
			//Find the shortest distance of this point with the kCenters.
			for (int i = 0; i < kCenters.size(); i ++) {
				ArrayList<Float> center = kCenters.get(i);
				double dist = distanceOf(thisPoint, center);
				if (dist < minDistance) {
					tag = i;
					minDistance = dist;
				}
			}
			
			//Add the value of this vertex into the sum of tag class.
			ArrayList<Float> sum = contents.get(tag);
			for (int i = 0; i < dimension; i ++) {
				sum.set(i, thisPoint.get(i));
			}
			//Init the refered class's number to 1.
			nums = contents.get(K);
			nums.set(tag, 1.0f);
			
		}
		*//**
		 * Else, directly init the sums and nums based on the tag that
		 * has been placed on the vertex's value in the computing.
		 *//*
		else {
			
			KCentersAggregateValue kCentersAgg = (KCentersAggregateValue) context.getAggregateValue(KMeansBSP.AGGREGATE_KCENTERS);
			ArrayList<ArrayList<Float>> newKCenters = new ArrayList<ArrayList<Float>>();
			
			//Calculate the new k centers and save them to newKCenters.
			ArrayList<ArrayList<Float>> contents1 = kCentersAgg.getValue();
			ArrayList<Float> nums1 = contents1.get(K);
			for (int i = 0; i < K; i ++) {
				ArrayList<Float> center = new ArrayList<Float>();
				//Get the sum of coordinates of points in class i.
				ArrayList<Float> sum = contents1.get(i);
				//Get the number of points in class i.
				float num = nums1.get(i);
				for (int j = 0; j < dimension; j ++) {
					//the center's coordinate value.
					center.add(sum.get(j)/num);
				}
				//The i center.
				newKCenters.add(center);
			}
			
			//Calculate the class tag of this vertex.
			int tag = 0;
			double minDistance = Float.MAX_VALUE;
			//Find the shortest distance of this point with the kCenters.
			for (int i = 0; i < this.kCenters.size(); i ++) {
				ArrayList<Float> center = this.kCenters.get(i);
				double dist = distanceOf(thisPoint, center);
				if (dist < minDistance) {
					tag = i;
					minDistance = dist;
				}
			}
			
			//Add the value of this vertex into the sum of tag class.
			ArrayList<Float> sum = this.contents.get(tag);
			for (int i = 0; i < dimension; i ++) {
				sum.set(i, thisPoint.get(i));
			}
			//Init the refered class's number to 1.
			nums = this.contents.get(K);
			nums.set(tag, 1.0f);
			
		}//end-if-else
*/	}

	@Override
	public void setValue(ArrayList<ArrayList<Float>> arg0) {
		this.contents = arg0;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		contents.clear();
		int centerSize = in.readInt();;
		for (int i = 0; i < centerSize; i ++) {
			int valueSize = in.readInt();
			ArrayList<Float> center = new ArrayList<Float>();
			for (int j = 0; j < valueSize; j ++) {
				center.add(in.readFloat());
			}
			contents.add(center);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		int centerSize = contents.size();
		out.writeInt(centerSize);
		for (int i = 0; i < centerSize; i ++) {
			int valueSize = contents.get(i).size();
			out.writeInt(valueSize);
			for (int j = 0; j < valueSize; j ++) {
				out.writeFloat(contents.get(i).get(j));
			}
		}
	}
	
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
		
		int superStepCount = context.getCurrentSuperStepCounter();
		this.kCenters.clear();
		if (superStepCount == 0) {
			BSPJob jobconf = context.getJobConf();
			K = Integer.valueOf(jobconf.get(KMeansBSP.KMEANS_K));
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
			dimension = kCenters.get(0).size();
		}
		else {
			KCentersAggregateValue kCentersAgg = (KCentersAggregateValue) context.getAggregateValue(KMeansBSP.AGGREGATE_KCENTERS);
			//Calculate the new k centers and save them to KCenters.
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
				this.kCenters.add(center);
			}
		}
	}
}
