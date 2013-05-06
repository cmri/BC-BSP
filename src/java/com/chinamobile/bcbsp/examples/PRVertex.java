/**
 * CopyRight by Chinamobile
 * 
 * PRVertex.java
 */
package com.chinamobile.bcbsp.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Vertex;

/**
 * Vertex implementation for PageRank.
 * 
 * @author Bai Qiushi
 * @version 1.0
 */
public class PRVertex extends Vertex<Integer, Float, PREdge> {

    int vertexID = 0;
    float vertexValue = 0.0f;
    List<PREdge> edgesList = new ArrayList<PREdge>();

    @Override
    public void addEdge(PREdge edge) {
        this.edgesList.add(edge);
    }

    @Override
    public void fromString(String vertexData) throws Exception {
        String[] buffer = new String[2];
        StringTokenizer str = new StringTokenizer(vertexData,
                Constants.KV_SPLIT_FLAG);
        if(str.hasMoreElements()){
            buffer[0]=str.nextToken();
        }else{
            throw new Exception();
        }
        if(str.hasMoreElements()){
            buffer[1]=str.nextToken();
        }
        str = new StringTokenizer(buffer[0], Constants.SPLIT_FLAG);
        if (str.countTokens() != 2) {
            throw new Exception();
        }

        this.vertexID = Integer.valueOf(str.nextToken());
        this.vertexValue = Float.valueOf(str.nextToken());

        if (buffer[1].length() > 1) { // There has edges.

            str = new StringTokenizer(buffer[1], Constants.SPACE_SPLIT_FLAG);
            while (str.hasMoreTokens()) {
                PREdge edge = new PREdge();
                edge.fromString(str.nextToken());
                this.edgesList.add(edge);
            }

        }
    }

    @Override
    public List<PREdge> getAllEdges() {
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
    public Float getVertexValue() {
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
        for (int i = 1; i < numEdges; i++) {
            buffer = buffer + Constants.SPACE_SPLIT_FLAG
                    + edgesList.get(i).intoString();
        }

        return buffer;
    }

    @Override
    public void removeEdge(PREdge edge) {
        this.edgesList.remove(edge);
    }

    @Override
    public void setVertexID(Integer vertexID) {
        this.vertexID = vertexID;
    }

    @Override
    public void setVertexValue(Float vertexValue) {
        this.vertexValue = vertexValue;
    }

    @Override
    public void updateEdge(PREdge edge) {
        removeEdge(edge);
        this.edgesList.add(edge);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.vertexID = in.readInt();
        this.vertexValue = in.readFloat();
        this.edgesList.clear();
        int numEdges = in.readInt();
        PREdge edge;
        for (int i = 0; i < numEdges; i++) {
            edge = new PREdge();
            edge.readFields(in);
            this.edgesList.add(edge);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.vertexID);
        out.writeFloat(this.vertexValue);
        out.writeInt(this.edgesList.size());
        for (PREdge edge : edgesList) {
            edge.write(out);
        }
    }

    @Override
    public int hashCode() {
        return Integer.valueOf(this.vertexID).hashCode();
    }
}
