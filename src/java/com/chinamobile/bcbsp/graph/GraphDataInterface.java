/**
 * CopyRight by Chinamobile
 * 
 * GraphDataInterface.java
 */
package com.chinamobile.bcbsp.graph;

import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.bspstaff.Staff;

/**
 * The Interface for GraphData, which manages
 * the graph data.
 * 
 * @author
 * @version
 */
public interface GraphDataInterface {

    /**
     * Add new vertex to the graph data.
     * This is defined for the graph data's loading stage.
     * 
     * @param vertex
     */
    @SuppressWarnings("unchecked")
    public void addForAll(Vertex vertex);
    
    /**
     * Because every staff owns a graph data, a graph
     *  data needs some properties of the staff.
     * @param staff
     */
    public void setStaff(Staff staff);
    
    /**
     * Initialize the graph data
     */
    public void initialize();
    
    /**
     * Get the size of the graph data.
     * This is defined for the traversal of the graph data
     * for the computation during the super steps.
     * 
     * @since 2012-3-6 aborted.
     * @return size
     */
    public int size();
    
    /**
     * Get the Vertex for a given index.
     * This is defined for the traversal of the graph data
     * for the computation during the super steps.
     * 
     * @since 2012-3-6 aborted.
     * @param index
     * @return headNode
     */
    @SuppressWarnings("unchecked")
    public Vertex get(int index);
    
    /**
     * Set the Vertex a for given index.
     * This is defined for the traversal of the graph data
     * for the computation during the super steps.
     * 
     * @param index
     * @param vertex
     */
    @SuppressWarnings("unchecked")
    public void set(int index, Vertex vertex, boolean activeFlag);
    
    /**
     * Get the size of all of the graph data.
     * This is defined for the traversal of the whole graph
     * data.
     * 
     * @return size
     */
    public int sizeForAll();
    
    /**
     * Get the Vertex for a given index for all.
     * This is defined for the traversal of the whole graph
     * data.
     * 
     * @param index
     * @return vertex
     */
    @SuppressWarnings("unchecked")
    public Vertex getForAll(int index);
    
    /**
     * Get the Vertex's active state flag for a given
     * index.
     * This is defined for the traversal of the whole
     * graph data.
     * 
     * @param index
     * @return
     */
    public boolean getActiveFlagForAll(int index);
    
    /**
     * Tell the graph data manager that add operation
     * is all over for the loading process.
     * Invoked after data load process.
     */
    public void finishAdd();
    
    /**
     * Tell the graph data manager to clean all
     * the resources for the graph data.
     * Invoked after the local computation and before
     * the staff to exit.
     */
    public void clean();
    
    /**
     * Get the total number of active vertices.
     * 
     * @return
     */
    public long getActiveCounter();

    /**
     * Log the current usage information of memory.
     */
    public void showMemoryInfo();
    
    /**
     * Get the total count of edges.
     * 
     * @return
     */
    public int getEdgeSize();
}
