/**
 * CopyRight by Chinamobile
 * 
 * GraphDataForMem.java
 */
package com.chinamobile.bcbsp.graph;

import java.util.ArrayList;
import java.util.List;

import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.bspstaff.Staff;

/**
 * GraphDataForMem implements GraphDataInterface for only
 * memory storage of the graph data.
 * 
 * @author
 * @version
 */
public class GraphDataForMem implements GraphDataInterface {

    @SuppressWarnings("unchecked")
    private List<Vertex> vertexList = new ArrayList<Vertex>();
    private List<Boolean> activeFlags = new ArrayList<Boolean>();
    private int totalVerticesNum = 0;
    private int totalEdgesNum = 0;
    private int globalIndex = 0;    
    
    // This version of GraphData doesn't need these two method.
    // However the interface requires them.
    public void setStaff(Staff staff){};
    public void initialize(){};
    
    @SuppressWarnings("unchecked")
    @Override
    public synchronized void addForAll(Vertex vertex) {
        vertexList.add(vertex);
        activeFlags.add(true);
        this.totalEdgesNum = this.totalEdgesNum + vertex.getEdgesNum();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Vertex get(int index) {
       Vertex vertex = null;
        
        do {
            if (activeFlags.get(globalIndex)) {
                vertex = vertexList.get(globalIndex);
                break;
            }
            globalIndex++;
        } while (globalIndex < totalVerticesNum);

        return vertex;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Vertex getForAll(int index) {
        return vertexList.get(index);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void set(int index, Vertex vertex, boolean activeFlag) {
        this.vertexList.set(index, vertex);
        activeFlags.set(index, activeFlag);
        globalIndex++;
        if (globalIndex >= totalVerticesNum) {
            resetGlobalIndex();
        }
    }

    @Override
    public int size() {
        updateTotalVertices();
        return (int)getActiveCounter();
    }

    @Override
    public int sizeForAll() {
        updateTotalVertices();
        return totalVerticesNum;
    }

    @Override
    public void clean() {
        vertexList.clear();
        activeFlags.clear();
        this.totalEdgesNum = 0;
        updateTotalVertices();
        resetGlobalIndex();
    }

    @Override
    public void finishAdd() {
        // For memory-only version, do nothing.
    }

    @Override
    public long getActiveCounter() {
        long counter = 0;
        for (boolean flag : activeFlags) {
            if (flag) {
                counter++;
            }
        }
        
        return counter;
    }
    
    private void resetGlobalIndex() {
        globalIndex = 0;
    }
    
    private void updateTotalVertices() {
        totalVerticesNum = this.vertexList.size();
    }

    @Override
    public boolean getActiveFlagForAll(int index) {
        
        return activeFlags.get(index);
    }

    @Override
    public void showMemoryInfo() {
  
    }

    @Override
    public int getEdgeSize() {
        return this.totalEdgesNum;
    }
}
