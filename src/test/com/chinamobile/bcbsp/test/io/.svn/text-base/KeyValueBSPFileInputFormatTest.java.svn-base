/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.io;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.EdgeInterface;
import com.chinamobile.bcbsp.api.VertexInterface;
import com.chinamobile.bcbsp.examples.StringEdge;
import com.chinamobile.bcbsp.examples.StringVertex;
import com.chinamobile.bcbsp.io.KeyValueBSPFileInputFormat.KVRecordReader;

public class KeyValueBSPFileInputFormatTest extends TestCase {

    @SuppressWarnings("unused")
    private KVRecordReader vr;
    private Configuration conf;
    private TaskAttemptContext tac;

     public void setUp() throws IOException, InterruptedException {
     vr = mock(KVRecordReader.class);

     conf = new Configuration();
     conf.setClass(Constants.USER_BC_BSP_JOB_VERTEX_CLASS,
     StringVertex.class, VertexInterface.class);
     conf.setClass(Constants.USER_BC_BSP_JOB_EDGE_CLASS, StringEdge.class,
     EdgeInterface.class);
     tac = mock(TaskAttemptContext.class);
     when(tac.getConfiguration()).thenReturn(conf);
    
     }

    public void testEdgesMustHaveValues() throws IOException,
            InterruptedException {
        String input = "vertex:1\tedge1";
        StringVertex vertex = new StringVertex();
        try {
            vertex.fromString(input);
            fail("It's edge have no values, should have thrown an Exception!");
        } catch (Exception e) {
            // e.printStackTrace();
            System.out.println("edges must have values!");
        }
    }

    public void testEdgesNum() throws Exception {
        String input = "vertex1:1\tedge1:1 edge2:2 edge3:3";
        StringVertex expectedvertex = new StringVertex();
        StringVertex vertex = new StringVertex();
        expectedvertex.fromString(input);
        vertex.setVertexID("vertex1");
        vertex.setVertexValue("1");
        StringEdge edge = new StringEdge();
        edge.setVertexID("edge1");
        edge.setEdgeValue("1");
        vertex.addEdge(edge);
        edge.setVertexID("edge2");
        edge.setEdgeValue("2");
        vertex.addEdge(edge);
        edge.setVertexID("edge3");
        edge.setEdgeValue("3");
        vertex.addEdge(edge);
        // System.out.println(vertex.getEdgesNum());
        assertEquals(expectedvertex.getEdgesNum(), vertex.getEdgesNum());
    }

}
