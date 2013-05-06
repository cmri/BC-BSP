/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.io;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.examples.StringEdge;
import com.chinamobile.bcbsp.examples.StringVertex;
import com.chinamobile.bcbsp.io.TextBSPFileOutputFormat.LineRecordWriter;

public class TextBSPFileOutputFormatTest extends TestCase {

    public void testVertexWithNoEdges() throws IOException,
            InterruptedException {
        Configuration conf = new Configuration();
        TaskAttemptContext tac = mock(TaskAttemptContext.class);
        when(tac.getConfiguration()).thenReturn(conf);

        StringVertex vertex = new StringVertex();
        String VertexID = "testVertexWithNoEdges-VertexID";
        String VertexValue = "testVertexWithNoEdges-VertexValue";

        vertex.setVertexID(VertexID);
        vertex.setVertexValue(VertexValue);

        Text key = new Text(vertex.intoString());
        Text value = null;
        
        LineRecordWriter writer = mock(LineRecordWriter.class);
        writer.write(key, value);

        Text expectedkey = new Text(
                "testVertexWithNoEdges-VertexID:testVertexWithNoEdges-VertexValue"
                        + Constants.KV_SPLIT_FLAG);
        Text expectedvalue = null;

        verify(writer).write(expectedkey, expectedvalue);

        System.out.println(key.toString() + value);

        assertEquals(expectedkey.toString(), key.toString());

    }

    public void testVertexWithEdges() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        TaskAttemptContext tac = mock(TaskAttemptContext.class);
        when(tac.getConfiguration()).thenReturn(conf);

        StringVertex vertex = new StringVertex();
        String VertexID = "testVertexWithEdges-VertexID";
        String VertexValue = "testVertexWithEdges-VertexValue";
        vertex.setVertexID(VertexID);
        vertex.setVertexValue(VertexValue);
        Text key = new Text(vertex.intoString());
        Text value = null;

        List<StringEdge> edgesList = new ArrayList<StringEdge>();
        edgesList.add(mockEdge("edge1", "1"));
        edgesList.add(mockEdge("edge2", "2"));
        value = new Text(edgesList.get(0).intoString());

        for (int i = 1; i < edgesList.size(); i++) {
            value = new Text(value + Constants.SPACE_SPLIT_FLAG
                    + edgesList.get(i).intoString());
        }
        LineRecordWriter writer = mock(LineRecordWriter.class);
        writer.write(key, value);
        Text expectedkey = new Text(
                "testVertexWithEdges-VertexID:testVertexWithEdges-VertexValue"
                        + Constants.KV_SPLIT_FLAG);
        Text expectedvalue = new Text("edge1:1 edge2:2");
        verify(writer).write(expectedkey, expectedvalue);
        System.out.println(key.toString() + value.toString());
        assertEquals(expectedkey.toString() + expectedvalue.toString(), key
                .toString()
                + value.toString());

    }

    private StringEdge mockEdge(String id, String value) {
        StringEdge stringEdge = new StringEdge();
        stringEdge.setVertexID(id);
        stringEdge.setEdgeValue(value);
        return stringEdge;
    }

}
