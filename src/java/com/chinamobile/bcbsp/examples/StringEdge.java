/**
 * CopyRight by Chinamobile
 * 
 * StringEdge.java
 */
package com.chinamobile.bcbsp.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Edge;

/**
 * Edge implementation with type of String.
 * 
 * @author Bai Qiushi
 * @version 1.0
 */
public class StringEdge extends Edge<String, String> {

    String vertexID = null;
    String edgeValue = null;

    @Override
    public void fromString(String edgeData) throws Exception {
        StringTokenizer str = new StringTokenizer(edgeData,
                Constants.SPLIT_FLAG);
        if (str.countTokens() != 2)
            throw new Exception();
        this.vertexID = str.nextToken();
        this.edgeValue = str.nextToken();
    }

    @Override
    public String getEdgeValue() {
        return this.edgeValue;
    }

    @Override
    public String getVertexID() {
        return this.vertexID;
    }

    @Override
    public String intoString() {
        return this.vertexID + Constants.SPLIT_FLAG + this.edgeValue;
    }

    @Override
    public void setEdgeValue(String edgeValue) {
        this.edgeValue = edgeValue;
    }

    @Override
    public void setVertexID(String vertexID) {
        this.vertexID = vertexID;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.vertexID = Text.readString(in);
        this.edgeValue = Text.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this.vertexID);
        Text.writeString(out, this.edgeValue);
    }

    @Override
    public boolean equals(Object object) {
        StringEdge edge = ( StringEdge ) object;
        if (this.vertexID.equals(edge.getVertexID())) {
            return true;
        } else {
            return false;
        }
    }
}
