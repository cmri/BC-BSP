/**
 * CopyRight by Chinamobile
 * 
 * RecordParseDefault.java
 */
package com.chinamobile.bcbsp.partition;

import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.RecordParse;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.util.BSPJob;
/**
 * RecordParseDefault
 *  
 * This class is used to parse a record as a HeadNode object.  
 */
public class RecordParseDefault extends RecordParse {
    // For Log
    @SuppressWarnings("unused")
    private static final Log LOG = LogFactory.getLog(RecordParseDefault.class);

    private Class<? extends Vertex<?, ?, ?>> vertexClass;
    
    /**
     * This method is used to parse a record as a Vertex object.
     * 
     * @param recordReader
     * @param headNode
     * @return a Vertex object
     */
    @SuppressWarnings("unchecked")
    @Override
    public Vertex recordParse(String key, String value) {
        Vertex vertex = null;

        try {
            vertex = vertexClass.newInstance();
            vertex.fromString(key + Constants.KV_SPLIT_FLAG + value);

        } catch (Exception e) {
            LOG.error("RecordParse", e);
            return null;
        }
        return vertex;
    }
    /**
     * This method is used to parse a record and obtain VertexID .
     * @param recordReader
     * @return the vertex id
     */
    @Override
    public Text getVertexID(Text key) {
        try {
            StringTokenizer str = new StringTokenizer(key.toString(),
                    Constants.SPLIT_FLAG);
            if(str.countTokens()!=2) return null;
            return new Text(str.nextToken());
        } catch (Exception e) {
            return null;
        }
    }
    /**
     * This method is used to initialize the RecordParseDefault.
     * 
     * @param job
     */
    @Override
    public void init(BSPJob job) {
        this.vertexClass = job.getVertexClass();
    }
}
