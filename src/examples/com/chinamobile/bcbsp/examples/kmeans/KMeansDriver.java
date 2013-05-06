/**
 * KMeansDriver.java
 */
package com.chinamobile.bcbsp.examples.kmeans;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.hadoop.fs.Path;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.io.KeyValueBSPFileInputFormat;
import com.chinamobile.bcbsp.io.TextBSPFileOutputFormat;
import com.chinamobile.bcbsp.util.BSPJob;

/**
 * KMeansDriver
 * This is used to drive the KMeans example.
 * 
 * @author Bai Qiushi
 * @version 1.0 2012-2-29
 */
public class KMeansDriver {

	/**
	 * Note:
	 * To use this KMeansBSP example:
	 * (1)Must set the "KMEANS_K" value into the job configuration.
	 * 		
	 * 		e.g. BSPJob job.set(KMeansBSP.KMEANS_K, String.valueOf(x));
	 * 
	 * (2)Must set the "KMEANS_CENTERS" value into the job configuration.
	 * 		
	 * 		e.g. BSPJob job.set(KMeansBSP.KMEANS_KCENTERS, "x11-x12-...-x1n|x21-x22-...-x2n|...|xk1-xk2-...-xkn");
	 * 
	 * (3)Must register the "AGGREGATE_KCENTERS" aggregator into the job configuration.
	 * 
	 * 		use: BSPJob registerAggregator(KMeansBSP.AGGREGATE_KCENTERS, KCentersAggregator.class, KCentersAggregateValue.class);
	 * 
	 * (4)Must register the "AGGREGATE_ERRORS" aggregator into the job configuration.
	 * 
	 * 		use: BSPJob registerAggregator(
	 */
	
	public static void main(String[] args) throws Exception {
		
		if (args.length < 5) {
			System.out.println("Usage: <nSupersteps>  <FileInputPath>  <FileOutputPath>  <K>  <K-Centers FilePath>" +
					"  <SplitSize(MB)> <PartitionNum> <SendThreshold>  <SendCombineThreshold> " +
					"  <MemDataPercent>  <Beta>  <HashBucketNum>  <MsgPackSize>");
			System.exit(-1);
		}
		
		// Set the base configuration for the job
		BSPConfiguration conf = new BSPConfiguration();
		BSPJob bsp = new BSPJob(conf, KMeansDriver.class);
		bsp.setJobName("KMeans");
		//bsp.setNumPartition(2);
		bsp.setNumSuperStep(Integer.parseInt(args[0]));
		bsp.setPartitionType(Constants.PARTITION_TYPE.HASH);
		bsp.setPriority(Constants.PRIORITY.NORMAL);
		bsp.setWritePartition(com.chinamobile.bcbsp.partition.NotDivideWritePartition.class);
		
		// Set the BSP.class
		bsp.setBspClass(KMeansBSP.class);
		bsp.setVertexClass(KMVertex.class);
		bsp.setEdgeClass(KMEdge.class);
		
		// Set the InputFormat.class and OutputFormat.classl
		bsp.setInputFormatClass(KeyValueBSPFileInputFormat.class);
		bsp.setOutputFormatClass(TextBSPFileOutputFormat.class);
		
		// Set the InputPath and OutputPath
		KeyValueBSPFileInputFormat.addInputPath(bsp, new Path(args[1]));
		TextBSPFileOutputFormat.setOutputPath(bsp, new Path(args[2]));
		
		// Set the graph data implementation version as disk version.
		bsp.setGraphDataVersion(bsp.DISK_VERSION);
		// Set the message queues implementation version as disk version.
		bsp.setMessageQueuesVersion(bsp.DISK_VERSION);
		
		// Register the aggregator.
		bsp.registerAggregator(KMeansBSP.AGGREGATE_KCENTERS, KCentersAggregator.class, KCentersAggregateValue.class);
		
		// Set the kmeans.k
		bsp.set(KMeansBSP.KMEANS_K, String.valueOf(args[3]));
		
		File kCentersFile = new File(args[4]);
		if (!kCentersFile.exists()) {
			System.out.println("K-Centers FilePath does not exist!");
			System.exit(-1);
		}
		FileReader fr = new FileReader(kCentersFile);
		BufferedReader br = new BufferedReader(fr);
		String kCenters = br.readLine();
		//Give seed k centers.
		//String kCenters = "5.4-3.7-1.5-0.2|6.1-2.8-4.7-1.2|7.7-3.0-6.1-2.3";
		
		// Set the kmeans.kcenters
		bsp.set(KMeansBSP.KMEANS_CENTERS, kCenters);
		
		if (args.length > 5) {
			bsp.setSplitSize(Integer.valueOf(args[5]));
		}
		if (args.length > 6) {
			bsp.setNumPartition(Integer.parseInt(args[6]));
		}
		if (args.length > 7) {
			bsp.setSendThreshold(Integer.parseInt(args[7]));
		}
		if (args.length > 8) {
			bsp.setSendCombineThreshold(Integer.parseInt(args[8]));
		}
		if (args.length > 9) {
			bsp.setMemoryDataPercent(Float.parseFloat(args[9]));
		}
		if (args.length > 10) {
			bsp.setBeta(Float.parseFloat(args[10]));
		}
		if (args.length > 11) {
			bsp.setHashBucketNumber(Integer.parseInt(args[11]));
		}
		if (args.length > 12) {
			bsp.setMessagePackSize(Integer.parseInt(args[12]));
		}
		
		// Summit the job!
		bsp.waitForCompletion(true);
	}

}
