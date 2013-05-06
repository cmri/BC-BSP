/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.graph;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.examples.PREdge;
import com.chinamobile.bcbsp.examples.PRVertex;
import com.chinamobile.bcbsp.graph.GraphDataForDisk;
import com.chinamobile.bcbsp.graph.GraphDataForMem;
import com.chinamobile.bcbsp.graph.GraphDataInterface;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

@RunWith(Parameterized.class)
public class GraphDataInterfaceTest {

    private GraphDataInterface graphdata;
    private static String[] datas = {
        "0:10.0\t3:0 1:0 2:0 4:0 0:0 0:0 0:0 0:0 0:0 1:0 0:0 1:0 3:0",
        "1:10.0\t4:0 2:0 0:0 2:0 4:0 1:0 2:0 1:0 1:0 3:0 3:0 3:0 4:0 0:0",
        "2:10.0\t2:0 1:0 4:0 2:0 3:0 3:0 0:0 3:0 0:0 1:0 3:0 4:0 1:0",
        "3:10.0\t4:0 2:0 3:0 0:0 0:0 1:0 2:0 0:0 1:0 2:0 0:0 4:0 0:0 1:0",
        "4:10.0\t4:0 0:0 1:0 1:0 4:0 2:0 1:0 1:0 0:0 4:0 1:0 2:0 3:0 0:0 2:0 4:0"};
    private static BSPJob job;
    private static int partitionId;
    @SuppressWarnings("unchecked")
    private static Class vertexClass = PRVertex.class;
    @SuppressWarnings("unchecked")
    private static Class edgeClass = PREdge.class;
    public GraphDataInterfaceTest(GraphDataInterface graphdata){
        
        this.graphdata = graphdata;
    }
    @SuppressWarnings("unchecked")
    @Parameters
    public static Collection<Object[]> getParameters(){
        job = mock(BSPJob.class);
        when(job.getMemoryDataPercent()).thenReturn(0.8f);
        when(job.getBeta()).thenReturn(0.5f);
        when(job.getHashBucketNumber()).thenReturn(32);
        when(job.getJobID()).thenReturn(new BSPJobID("jtIdentifier",2));
        when(job.getVertexClass()).thenReturn(vertexClass);
        when(job.getEdgeClass()).thenReturn(edgeClass);
        job.getBeta();
        partitionId = 1;
        GraphDataForDisk graphDataForDisk = new GraphDataForDisk();
        graphDataForDisk.initialize(job, partitionId);
        return Arrays.asList(new Object[][] {
                { graphDataForDisk},
                { new GraphDataForMem()}
        });
    }
    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception{

        for(int i = 0 ;i < 4;i++){//At setUp, we add 4 vertice.Those vertice contain 54 edges.
            Vertex vertex = new PRVertex();
            vertex.fromString(datas[i]);
            graphdata.addForAll(vertex);
        }

    }
    @After
    public void tearDown(){
        graphdata.clean();
        System.out.println("size:"+graphdata.sizeForAll());
    }
    @SuppressWarnings("unchecked")
    @Test
    public void testAddForAll() throws Exception {
       
        int sizeBeforAdd = graphdata.size();
        Vertex vertex = new PRVertex();
        vertex.fromString(datas[4]);
        graphdata.addForAll(vertex);
        assertEquals("After add 1 vertex, there are size+1 vertice in graphdata.",
                sizeBeforAdd + 1, graphdata.size());
    }

    @Test
    public void testSize() {
        assertEquals("After setUp, there are 4 vertice in graphdata.",
                4, graphdata.size());
    }

    @Test
    public void testGet() {
        graphdata.size();//In GraphDataForMem, it will be called before graphdata.get(index)

        if(graphdata instanceof GraphDataForDisk ){
            assertEquals("Check getForAll.",
                    new Integer(1), ( Integer ) graphdata.get(0).getVertexID());
        }
        if(graphdata instanceof GraphDataForMem ){
            graphdata.get(1);
            assertEquals("Check getForAll.",
                    new Integer(0), ( Integer ) graphdata.get(0).getVertexID());
        }
    }

    @Test
    public void testSet() {
        PRVertex vertex = (PRVertex) graphdata.get(0);
        float oldValue = vertex.getVertexValue();
        vertex.setVertexValue(oldValue + 1);
        graphdata.set(0, vertex, true);
        assertEquals("After set, the value of 0th vertex has changed.",
                oldValue + 1, graphdata.get(0).getVertexValue());
    }

    @Test
    public void testSizeForAll() {
        assertEquals("After setUp, there are 4 vertice in graphdata.",
                4, graphdata.size());
    }

    @Test
    public void testGetForAll() {
        graphdata.size();//In GraphDataForMem, it will be called before graphdata.get(index)
        PRVertex vertex = (PRVertex) graphdata.getForAll(0);
        
        float oldValue = vertex.getVertexValue();
        vertex.setVertexValue(oldValue + 1);
        
        graphdata.set(0, vertex, false);
        //after set, there are 3 active vertice.
        if(graphdata instanceof GraphDataForDisk ){
            assertEquals("Check getForAll.",
                    new Integer(3), ( Integer ) graphdata.getForAll(1).getVertexID());
        }
        if(graphdata instanceof GraphDataForMem ){
            graphdata.get(1);
            assertEquals("Check getForAll.",
                    new Integer(1), ( Integer ) graphdata.getForAll(1).getVertexID());
        }

    }

    @Test
    public void testGetActiveFlagForAll() {
        assertEquals("Check getActive.",
                true, graphdata.getActiveFlagForAll(0));
        PRVertex vertex = (PRVertex) graphdata.get(0);
        graphdata.set(0, vertex, false);
        assertEquals("Check getActive.",
                false, graphdata.getActiveFlagForAll(0));
    }

    @Test
    
    public void testFinishAdd() {

    }

    @Test
    public void testClean() {
        graphdata.clean();
        assertEquals("Check clean.",
                0, graphdata.size());
        assertEquals("Check clean.",
                0, graphdata.sizeForAll());
        assertEquals("Check clean.",
                0,graphdata.getActiveCounter());
        assertEquals("Check clean.",
                0,graphdata.getEdgeSize());
    }

    @Test
    public void testGetActiveCounter() {
        assertEquals("After setUp, there are 4 active vertice.",
                4, graphdata.getActiveCounter());
        PRVertex vertex = (PRVertex) graphdata.get(0);
        graphdata.set(0, vertex, false);
        assertEquals("After set one of them false, there are 3 active vertice.",
                3, graphdata.getActiveCounter());
        
    }

    @Test
    public void testShowMemoryInfo() {
        graphdata.showMemoryInfo();//Only test in Disk version
    }

    @Test
    public void testGetEdgeSize() {
        assertEquals("Check edge size.",
                54, graphdata.getEdgeSize());
    }

}