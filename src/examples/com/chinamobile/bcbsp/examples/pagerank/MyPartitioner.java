package com.chinamobile.bcbsp.examples.pagerank;

import java.util.zip.CRC32;

import org.apache.hadoop.io.Text;
import com.chinamobile.bcbsp.api.Partitioner;

class MyPartitioner extends Partitioner<Text> {
	public MyPartitioner(){}
	public MyPartitioner(int numPartition) {
		this.numPartition = numPartition;
	}

	@Override
	public int getPartitionID(Text url) {
		// TODO Auto-generated method stub
		CRC32 checksum = new CRC32();
		checksum.update(url.toString().getBytes());
		int crc = (int) checksum.getValue();
		long hashcode = (crc >> 16) & 0x7fff;
		return (int) (hashcode % this.numPartition);
	}
}
