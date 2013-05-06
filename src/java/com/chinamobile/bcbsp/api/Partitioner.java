/**
 * CopyRight by Chinamobile
 * 
 * Partitioner.java
 */
package com.chinamobile.bcbsp.api;

import org.apache.hadoop.mapreduce.InputSplit;

import com.chinamobile.bcbsp.util.BSPJob;

/**
 * Partition
 * 
 * This abstract class is the primary interface for users to define their own
 * hash method.The user must provide a no-argument constructor.
 * 
 * @author
 * @version
 * 
 * @param <IDT>
 */
public abstract class Partitioner<IDT> {
    
    protected int numPartition;

    public void setNumPartition(int numPartition) {
        this.numPartition = numPartition;
    };

    /**
     * This method is the primary method for users to implement hash
     * method.Through the vertex id hash calculated for the respective partition
     * id.Finally,return partition's id.
     * 
     * @param id HeadNode's id
     * @return the partition's id witch this vertex belongs to partition
     */
    public abstract int getPartitionID(IDT id);
    public void intialize(BSPJob job,InputSplit split){};
}
