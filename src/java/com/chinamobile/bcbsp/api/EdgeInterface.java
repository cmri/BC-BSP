/**
 * CopyRight by Chinamobile
 * 
 * EdgeInterface.java
 */
package com.chinamobile.bcbsp.api;

/**
 * Edge interface.
 * 
 * @author Bai Qiushi
 * @version 1.0
 */
public interface EdgeInterface<K,V2> {

    /**
     * Set the destination vertex ID.
     * 
     * @param vertexID
     */
    public void setVertexID(K vertexID);
    
    /**
     * Get the destination vertex ID.
     * 
     * @return vertexID
     */
    public K getVertexID();
    
    /**
     * Set the edge value.
     * 
     * @param edgeValue
     */
    public void setEdgeValue(V2 edgeValue);
    
    /**
     * Get the edge value.
     * 
     * @return edgeValue
     */
    public V2 getEdgeValue();
    
    /**
     * Transform into a String.
     * 
     * @return
     */
    public String intoString();
    
    /**
     * Transform from a String.
     * 
     * @param edgeData
     * @throws Exception 
     */
    public void fromString(String edgeData) throws Exception;
    
    /**
     * equals
     * 
     * @param object
     * @return boolean
     */
    public boolean equals(Object object);
    
}
