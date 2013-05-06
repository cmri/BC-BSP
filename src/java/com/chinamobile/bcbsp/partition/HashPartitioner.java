/**
 * CopyRight by Chinamobile
 * 
 * HashPartitioner.java
 */
package com.chinamobile.bcbsp.partition;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.chinamobile.bcbsp.api.Partitioner;
/**
 * HashPartitioner is partitioning by the MD5-based hashcode of the vertex id.
 * 
 */
public class HashPartitioner<IDT> extends Partitioner<IDT> {

    public HashPartitioner() {
    }

    public HashPartitioner(int numPartition) {
        this.numPartition = numPartition;
    }
  /**
   * Partitions a vertex accroding the id mapping to a partition.
   * 
   * @param id the vertex id
   * @return a number between 0 and numPartition that tells which
   *         partition it belongs to.
   */
    @Override
    public int getPartitionID(IDT id) {
        String url = id.toString();
        MessageDigest md5 = null;
        if (md5 == null) {
            try {
                md5 = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {

                throw new IllegalStateException("++++ no md5 algorythm found");
            }
        }

        md5.reset();
        md5.update(url.getBytes());
        byte[] bKey = md5.digest();
        long hashcode = (( long ) (bKey[3] & 0xFF) << 24)
                | (( long ) (bKey[2] & 0xFF) << 16)
                | (( long ) (bKey[1] & 0xFF) << 8) | ( long ) (bKey[0] & 0xFF);
        int result = (int) (hashcode % this.numPartition);
        result = (result < 0? result + this.numPartition: result);
        return result;
    }

}
