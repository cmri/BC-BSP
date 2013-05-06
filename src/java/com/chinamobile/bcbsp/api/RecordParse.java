/**
 * CopyRight by Chinamobile
 * 
 * RecordParse.java
 */
package com.chinamobile.bcbsp.api;

import org.apache.hadoop.io.Text;

import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.util.BSPJob;

/**
 * This class is used to parse a record as a HeadNode object. * 
 */
public abstract class RecordParse {
    
    /**
     * This method is used to initialize the RecordParse extended.
     * 
     * @param job
     */
    public abstract void init(BSPJob job);
    
    /**
     * This method is used to parse a record as a Vertex object.
     * 
     * @param recordReader
     * @param headNode
     */
    @SuppressWarnings("unchecked")
    public abstract Vertex recordParse(String key, String value);
    
    /**
     * This method is used to parse a record and obtain VertexID .
     * @param recordReader
     * @return
     */
    public abstract Text getVertexID(Text key);    
}
