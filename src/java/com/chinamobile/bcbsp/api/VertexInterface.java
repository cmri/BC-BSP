/**
 * CopyRight by Chinamobile
 * 
 * VertexInterface.java
 */
package com.chinamobile.bcbsp.api;

import java.util.List;

/**
 * Vertex interface.
 * 
 * @author Bai Qiushi
 * @version 1.0
 */
public interface VertexInterface<K, V1, E> {

    /**
     * Set the vertex ID.
     * 
     * @param vertexID
     */
    public void setVertexID(K vertexID);
    
    
    /**
     * Get the vertex ID.
     * 
     * @return vertexID
     */
    public K getVertexID();
    
    /**
     * Set the vertex value.
     * 
     * @param vertexValue
     */
    public void setVertexValue(V1 vertexValue);
    
    /**
     * Get the vertex value.
     * 
     * @return vertexValue
     */
    public V1 getVertexValue();
    
    /**
     * Add an edge to the edge list.
     * 
     * @param edge
     */
    public void addEdge(E edge);
    
    /**
     * Get the whole list of edges of the vertex.
     * 
     * @return List of edges
     */
    public List<E> getAllEdges();
    
    /**
     * Remove the edge from the edge list.
     * 
     * @param edge
     */
    public void removeEdge(E edge);
    
    /**
     * Update the edge.
     * 
     * @param edge
     */
    public void updateEdge(E edge);
    
    /**
     * Get the number of edges.
     * 
     * @return
     */
    public int getEdgesNum();
    
    /**
     * Hash code.
     * 
     * @return int
     */
    public int hashCode();
    
    /**
     * Transform into a String.
     * 
     * @return
     */
    public String intoString();
    
    /**
     * Transform from a String.
     * 
     * @param vertexData
     * @throws Exception 
     */
    public void fromString(String vertexData) throws Exception;
}
