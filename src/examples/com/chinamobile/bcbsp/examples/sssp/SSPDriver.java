package com.chinamobile.bcbsp.examples.sssp;

/**
 * PageRankDriver.java
 */
import org.apache.hadoop.fs.Path;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.io.KeyValueBSPFileInputFormat;
import com.chinamobile.bcbsp.io.TextBSPFileOutputFormat;

/**
 * PageRankDriver.java This is used to drive the PageRank example.
 * 
 * @author LiBingLiang
 * @version 1.0 2012-3-6
 */

public class SSPDriver {

	public static void main(String[] args) throws Exception {
		
		if (args.length < 4) {
			System.out.println("Usage:<SourceID>  <nSupersteps>  <FileInputPath>  <FileOutputPath>  <SplitSize(MB)> <PartitionNum>" +
					"  <SendThreshold>  <SendCombineThreshold>  <MemDataPercent>  <Beta>  <HashBucketNum>  <MsgPackSize>");
			System.exit(-1);
		}

		// Set the base configuration for the job
		BSPConfiguration conf = new BSPConfiguration();
		BSPJob bsp = new BSPJob(conf, SSPDriver.class);
		bsp.setJobName("ShortestPath");
		bsp.setInt(SSP_BSP.SOURCEID, Integer.parseInt(args[0]));
		bsp.setNumSuperStep(Integer.parseInt(args[1]));
		bsp.setPartitionType(Constants.PARTITION_TYPE.HASH);
		bsp.setPriority(Constants.PRIORITY.NORMAL);
		if (args.length > 4) {
			bsp.setSplitSize(Integer.valueOf(args[4]));
		}
		if (args.length > 5) {
			bsp.setNumPartition(Integer.parseInt(args[5]));
		}
		if (args.length > 6) {
			bsp.setSendThreshold(Integer.parseInt(args[6]));
		}
		if (args.length > 7) {
			bsp.setSendCombineThreshold(Integer.parseInt(args[7]));
		}
		if (args.length > 8) {
			bsp.setMemoryDataPercent(Float.parseFloat(args[8]));
		}
		if (args.length > 9) {
			bsp.setBeta(Float.parseFloat(args[9]));
		}
		if (args.length > 10) {
			bsp.setHashBucketNumber(Integer.parseInt(args[10]));
		}
		if (args.length > 11) {
			bsp.setMessagePackSize(Integer.parseInt(args[11]));
		}

		// Set the BSP.class
		bsp.setBspClass(SSP_BSP.class);
		bsp.setVertexClass(SSPVertex.class);
		bsp.setEdgeClass(SSPEdge.class);
		
		bsp.setInputFormatClass(KeyValueBSPFileInputFormat.class);
		bsp.setOutputFormatClass(TextBSPFileOutputFormat.class);

		// Set the InputPath and OutputPath
		KeyValueBSPFileInputFormat.addInputPath(bsp, new Path(args[2]));
		TextBSPFileOutputFormat.setOutputPath(bsp, new Path(args[3]));

		// Set the combiner class.
		bsp.setCombiner(ShortestPathCombiner.class);

		// Set the graph data implementation version as disk version.
		bsp.setGraphDataVersion(bsp.DISK_VERSION);
		// Set the message queues implementation version as disk version.
		bsp.setMessageQueuesVersion(bsp.DISK_VERSION);
		// Set the beta param for the memory admin, so that graph data
		// occupies 40% of the memory and messages data occupies 60%.

		bsp.waitForCompletion(true);

	}
}
