/**
 * CopyRight by Chinamobile
 * 
 * BSPMessage.java
 */
package com.chinamobile.bcbsp.comm;

import java.io.Serializable;

import com.chinamobile.bcbsp.Constants;

/**
 * BSPMessage consists of the tag and the arbitrary amount of data to be
 * communicated.
 */
public class BSPMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /** the destination partition-ID */
    protected int dstPartition;
    /** the destination vertex-ID */
    protected String dstVertex;

    protected byte[] tag;
    protected byte[] data;

    public BSPMessage() {

    }

    /**
     * Constructor
     * 
     * @param tag
     *            of data
     * @param data
     *            of message
     */
    public BSPMessage(int dstPartition, String dstVertex, byte[] tag, byte[] data) {

        this.dstPartition = dstPartition;
        this.dstVertex = dstVertex;

        this.tag = new byte[tag.length];
        this.data = new byte[data.length];
        System.arraycopy(tag, 0, this.tag, 0, tag.length);
        System.arraycopy(data, 0, this.data, 0, data.length);
    }

    /**
     * Constructor
     * 
     * @param dstVertex
     * @param tag
     * @param data
     */
    public BSPMessage(String dstVertex, byte[] tag, byte[] data) {

        this.dstVertex = dstVertex;

        this.tag = new byte[tag.length];
        this.data = new byte[data.length];
        System.arraycopy(tag, 0, this.tag, 0, tag.length);
        System.arraycopy(data, 0, this.data, 0, data.length);
    }

    public BSPMessage(String dstVertex, byte[] data) {

        this.dstVertex = dstVertex;

        this.data = new byte[data.length];
        System.arraycopy(data, 0, this.data, 0, data.length);
        
        this.tag = new byte[0];
    }
    
    public BSPMessage(int dstPartition, String dstVertex, byte[] data) {
        
        this.dstPartition = dstPartition;
        this.dstVertex = dstVertex;

        this.data = new byte[data.length];
        System.arraycopy(data, 0, this.data, 0, data.length);
        
        this.tag = new byte[0];
    }

    public int getDstPartition() {
        return this.dstPartition;
    }

    public void setDstPartition(int partitionID) {
        this.dstPartition = partitionID;
    }

    public String getDstVertexID() {
        return this.dstVertex;
    }

    /**
     * BSP messages are typically identified with tags. This allows to get the
     * tag of data.
     * 
     * @return tag of data of BSP message
     */
    public byte[] getTag() {
        byte[] result = this.tag;
        return result;
    }

    /**
     * @return data of BSP message
     */
    public byte[] getData() {
        byte[] result = this.data;
        return result;
    }
    
    public String intoString() {
        String buffer = this.dstPartition + Constants.SPLIT_FLAG + 
                        this.dstVertex + Constants.SPLIT_FLAG + 
                        new String(this.tag) + Constants.SPLIT_FLAG + 
                        new String(this.data);
        return buffer;
    }
    
    public void fromString(String msgData) {
        String[] buffer = msgData.split(Constants.SPLIT_FLAG);
        
        this.dstPartition = Integer.parseInt(buffer[0]);
        this.dstVertex = buffer[1];
        this.tag = buffer[2].getBytes();
        this.data = buffer[3].getBytes();
    }
}
