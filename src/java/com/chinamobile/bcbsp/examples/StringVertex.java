/**
 * CopyRight by Chinamobile
 * 
 * StringVertex.java
 */
package com.chinamobile.bcbsp.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Vertex;

/**
 * Vertex implementation with type of String.
 * 
 * @author Bai Qiushi
 * @version 1.0
 */
public class StringVertex extends Vertex<String, String, StringEdge> {

    String vertexID = null;
    String vertexValue = null;
    List<StringEdge> edgesList = new ArrayList<StringEdge>();

    @Override
    public void addEdge(StringEdge edge) {
        this.edgesList.add(edge);
    }

    @Override
    public void fromString(String vertexData) throws Exception {
        String[] buffer = new String[2];
        StringTokenizer str = new StringTokenizer(vertexData,
                Constants.KV_SPLIT_FLAG);
        if (str.countTokens() != 2) {
            throw new Exception();
        }
        buffer[0] = str.nextToken();
        buffer[1] = str.nextToken();
        str = new StringTokenizer(buffer[0], Constants.SPLIT_FLAG);
        if (str.countTokens() != 2) {
            throw new Exception();
        }

        this.vertexID = str.nextToken();
        this.vertexValue = str.nextToken();

        if (buffer.length > 1) { // There has edges.

            str = new StringTokenizer(buffer[1], Constants.SPACE_SPLIT_FLAG);
            while (str.hasMoreTokens()) {
                StringEdge edge = new StringEdge();
                edge.fromString(str.nextToken());
                this.edgesList.add(edge);
            }

        }
    }

    @Override
    public List<StringEdge> getAllEdges() {
        return this.edgesList;
    }

    @Override
    public String getVertexID() {
        return this.vertexID;
    }

    @Override
    public String getVertexValue() {
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
    public void removeEdge(StringEdge edge) {
        this.edgesList.remove(edge);
    }

    @Override
    public void setVertexID(String vertexID) {
        this.vertexID = vertexID;
    }

    @Override
    public void setVertexValue(String vertexValue) {
        this.vertexValue = vertexValue;
    }

    @Override
    public void updateEdge(StringEdge edge) {
        removeEdge(edge);
        this.edgesList.add(edge);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.vertexID = Text.readString(in);
        this.vertexValue = Text.readString(in);
        this.edgesList.clear();
        int numEdges = in.readInt();
        StringEdge edge;
        for (int i = 0; i < numEdges; i++) {
            edge = new StringEdge();
            edge.readFields(in);
            this.edgesList.add(edge);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this.vertexID);
        Text.writeString(out, this.vertexValue);
        out.writeInt(this.edgesList.size());
        for (StringEdge edge : edgesList) {
            edge.write(out);
        }
    }

    @Override
    public int hashCode() {
        return vertexID.hashCode();
    }

    @Override
    public int getEdgesNum() {
        return this.edgesList.size();
    }
}
