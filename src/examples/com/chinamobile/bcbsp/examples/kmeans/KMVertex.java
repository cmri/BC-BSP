/**
 * KMVertex.java
 */
package com.chinamobile.bcbsp.examples.kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Vertex;

/**
 * KMVertex
 * Implementation of Vertex for K-Means
 * 
 * @author Bai Qiushi
 * @version 0.2
 */
public class KMVertex extends Vertex<Integer, Byte, KMEdge> {

	int vertexID = 0;
    byte vertexValue = 0;
    List<KMEdge> edgesList = new ArrayList<KMEdge>();
	
	@Override
	public void addEdge(KMEdge edge) {
		this.edgesList.add(edge);
	}

	@Override
	public void fromString(String vertexData) {
		String[] buffer = vertexData.split(Constants.KV_SPLIT_FLAG);
        String[] vBuffer = buffer[0].split(Constants.SPLIT_FLAG);
        this.vertexID = Integer.valueOf(vBuffer[0]);
        this.vertexValue = Byte.valueOf(vBuffer[1]);
        
        if (buffer.length > 1) { // There has edges.
            String[] eBuffer = buffer[1].split(Constants.SPACE_SPLIT_FLAG);
            for (int i = 0; i < eBuffer.length; i ++) {
                KMEdge edge = new KMEdge();
                edge.fromString(eBuffer[i]);
                this.edgesList.add(edge);
            }
        }
	}

	@Override
	public List<KMEdge> getAllEdges() {
		return this.edgesList;
	}

	@Override
	public int getEdgesNum() {
		return this.edgesList.size();
	}

	@Override
	public Integer getVertexID() {
		return this.vertexID;
	}

	@Override
	public Byte getVertexValue() {
		return this.vertexValue;
	}

	@Override
	public String intoString() {
		String buffer = vertexID + Constants.SPLIT_FLAG + vertexValue;
        buffer = buffer + Constants.KV_SPLIT_FLAG;
        int numEdges = edgesList.size();
        if (numEdges != 0) {
            buffer = buffer + edgesList.get(0).intoString();
        }
        for (int i = 1; i < numEdges; i ++) {
            buffer  = buffer + Constants.SPACE_SPLIT_FLAG + edgesList.get(i).intoString();
        }
        
        return buffer;
	}

	@Override
	public void removeEdge(KMEdge edge) {
		this.edgesList.remove(edge);
	}

	@Override
	public void setVertexID(Integer arg0) {
		this.vertexID = arg0;
	}

	@Override
	public void setVertexValue(Byte arg0) {
		this.vertexValue = arg0;
	}

	@Override
	public void updateEdge(KMEdge edge) {
		removeEdge(edge);
        this.edgesList.add(edge);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.vertexID = in.readInt();
        this.vertexValue = in.readByte();
        this.edgesList.clear();
        int numEdges = in.readInt();
        KMEdge edge;
        for (int i = 0; i < numEdges; i++) {
            edge = new KMEdge();
            edge.readFields(in);
            this.edgesList.add(edge);
        }
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.vertexID);
        out.writeByte(this.vertexValue);
        out.writeInt(this.edgesList.size());
        for (KMEdge edge : edgesList) {
            edge.write(out);
        }
	}

	@Override
    public int hashCode() {
        return Integer.valueOf(this.vertexID).hashCode();
    }
}
