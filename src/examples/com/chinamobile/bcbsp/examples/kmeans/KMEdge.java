/**
 * KMEdge.java
 */
package com.chinamobile.bcbsp.examples.kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Edge;

/**
 * KMEdge
 * Implementation of Edge for K-Means.
 * 
 * @author Bai Qiushi
 * @version 0.2
 */
public class KMEdge extends Edge<Byte, Float> {

	byte vertexID = 0;
    float edgeValue = 0;
	
	@Override
	public void fromString(String edgeData) {
		String[] buffer = edgeData.split(Constants.SPLIT_FLAG);
        this.vertexID = Byte.valueOf(buffer[0]);
        this.edgeValue = Float.valueOf(buffer[1]);
	}

	@Override
	public Float getEdgeValue() {
		return this.edgeValue;
	}

	@Override
	public Byte getVertexID() {
		return this.vertexID;
	}

	@Override
	public String intoString() {
		return this.vertexID + Constants.SPLIT_FLAG + this.edgeValue;
	}

	@Override
	public void setEdgeValue(Float arg0) {
		this.edgeValue = arg0;
	}

	@Override
	public void setVertexID(Byte arg0) {
		this.vertexID = arg0;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.vertexID = in.readByte();
        this.edgeValue = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(this.vertexID);
        out.writeFloat(edgeValue);
	}

	@Override
    public boolean equals(Object object) {
        KMEdge edge = (KMEdge) object;
        if (this.vertexID == edge.getVertexID()) {
            return true;
        } else {
            return false;
        }
    }
}
