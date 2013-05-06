/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.fault.storage;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.bspstaff.BSPStaff;
import com.chinamobile.bcbsp.examples.PREdge;
import com.chinamobile.bcbsp.examples.PRVertex;
import com.chinamobile.bcbsp.fault.storage.Checkpoint;
import com.chinamobile.bcbsp.graph.GraphDataFactory;
import com.chinamobile.bcbsp.graph.GraphDataForMem;
import com.chinamobile.bcbsp.graph.GraphDataInterface;
import com.chinamobile.bcbsp.io.TextBSPFileOutputFormat;
import com.chinamobile.bcbsp.test.BSPConfigurationSetter;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

public class TestCheckpoint extends TestCase {
    private static final Log LOG = LogFactory.getLog(TestCheckpoint.class);
    private Checkpoint checkpoint;
    private BSPConfiguration bspconf;
    private BSPJob bspjob;
    private FileSystem fs;
    private Path writepath;
    private BSPStaff bspstaff;
    String datapath = "/tmp/bcbsp_test";
    GraphDataForMem graphdatamem;
    GraphDataForMem graphdataforcheck;
    File bcbsp_test;

    @SuppressWarnings("unchecked")
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        // configure the configuration
        @SuppressWarnings("unused")
        String[] args = new String[] { "3", datapath + "input/",
                datapath + "output/" };
        bspconf = new BSPConfiguration();
        bspconf.set("fs.default.name", "file:///");
        bspconf.set("job.outputformat.class",TextBSPFileOutputFormat.class.getName());
        bspconf.set(Constants.USER_BC_BSP_JOB_GRAPH_DATA_VERSION, "0");
        bspconf.set("job.vertex.class", PRVertex.class.getName());
        bspconf.set("job.edge.class", PREdge.class.getName());
        // create job.xml
        int index = 0;
        bcbsp_test = new File(datapath + index);
        while(bcbsp_test.exists()){
            index++;
            bcbsp_test = new File(datapath + index);
        }
        bcbsp_test.mkdirs();
        File file = new File(bcbsp_test,  "jobfortest.xml");
        FileOutputStream fos = new FileOutputStream(file);
        DataOutputStream dos = new DataOutputStream(fos);
        bspconf.writeXml(dos);
        dos.close();
        bspjob = new BSPJob(new BSPJobID("TestCheckpoint", 0), file.toString());

        checkpoint = new Checkpoint(bspjob);

        bspstaff = new BSPStaff(){
            {
                graphDataFactory = new GraphDataFactory(bspconf);
            }
        };

        writepath = new Path(bcbsp_test.toString(), "checkpoint");

        graphdatamem = new GraphDataForMem();

        fillGraphData(graphdatamem, 10);
        randomizeGraphData(graphdatamem, 0);
        
        int size = graphdatamem.sizeForAll();
        graphdataforcheck = new GraphDataForMem();
        Vertex tmpvertex;
        @SuppressWarnings("unused")
        boolean tmpbool;
        for(int i = 0; i<size ; i++){
            tmpvertex = graphdatamem.getForAll(i);
            graphdataforcheck.addForAll(tmpvertex);
            if(!(tmpbool = graphdatamem.getActiveFlagForAll(i))){
                graphdataforcheck.set(i, tmpvertex, false);
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    public static void fillGraphData(GraphDataInterface graphdata, int vertexnum){
        Random rand = new Random(System.currentTimeMillis());
        int vertexcount = vertexnum;
        if(vertexnum == 0)
             vertexcount = rand.nextInt(1000);
        int edgecount = 0;
        for(int i = 0; i< vertexcount; i++){
            Vertex vertex = new PRVertex();
            vertex.setVertexID(i);
            edgecount = rand.nextInt(100);
            for(int j = 0; j< edgecount ; j++){
                PREdge edge = new PREdge();
                edge.setVertexID(rand.nextInt(vertexcount));
                edge.setEdgeValue((byte)rand.nextInt(250));
                vertex.addEdge(edge);
            }
            graphdata.addForAll(vertex);
        }
    }
    
    public static void randomizeGraphData(GraphDataInterface graphdata, double factor){
        Random rand = new Random(System.currentTimeMillis());
        double judge;
        int size = graphdata.sizeForAll();
        for(int i = 0; i< size;i++){
            judge = rand.nextDouble();
            if(judge < factor)
                graphdata.set(i, graphdata.getForAll(i), false);
        }
    }

    @After
    protected void tearDown() throws Exception {
        super.tearDown();
        try {
            fs = FileSystem.get(bspconf);
            boolean deleted = fs.delete(writepath, true);
            if (!deleted)
                throw new IOException();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.warn("Delete the writepath("
                    + writepath
                    + ") created by the testWriteCheckPoint() failed because of IOException:\t"
                    + e);
        }
        BSPConfigurationSetter.deleteRecursiveOnExist(bcbsp_test);
    }

    @Test
    public void testCheckpoint() {
        try {
            checkpoint.writeCheckPoint(graphdatamem, writepath, bspjob,
                    bspstaff);
        } catch (IOException e) {
            e.printStackTrace();
            fail("testWriteCheckpoint() failed because of IOException:\t" + e);
        }
        GraphDataInterface newgraphdatamem = checkpoint.readCheckPoint(
                writepath, bspjob, bspstaff);
        
        try{
            assertEquals(true, ifthesame(graphdataforcheck, newgraphdatamem));
        }catch(Exception e){
            LOG.info("assertEquals(true, ifthesame(graphdataforcheck, newgraphdatamem)) failed");
        }
    }

    @SuppressWarnings("unchecked")
    public static boolean ifthesame(GraphDataInterface graph1,
            GraphDataInterface graph2) {
        int graph1size = graph1.sizeForAll();
        int graph2size = graph2.sizeForAll();
        if (graph1size != graph2size) {
            LOG.info("sizeforall not equal");
            LOG.info("graph1 sizeforall:\t" + graph1.sizeForAll());
            LOG.info("graph2 sizeforall:\t" + graph2.sizeForAll());
            return false;
        } else if (graph1.getActiveCounter() != graph2.getActiveCounter()) {
            LOG.info("activecount not equal");
            LOG.info("graph1 true activecount:\t" + graph1.getActiveCounter());
            LOG.info("graph2 true activecount:\t" + graph2.getActiveCounter());
            return false;
        } else if (graph1.getEdgeSize() != graph2.getEdgeSize()) {
            LOG.info("edgesize not equal");
            LOG.info("graph1 edgesize:\t" + graph1.getEdgeSize());
            LOG.info("graph2 edgesize:\t" + graph2.getEdgeSize());
            return false;
        } else {
            for (int i = 0; i < graph1size; i++) {
                Vertex<?, ?, Edge> vertex1 = graph1.getForAll(i);
                Vertex<?, ?, Edge> vertex2 = graph2.getForAll(i);
                if (!vertex1.getVertexID().equals(vertex2.getVertexID())
                        || !vertex1.getVertexValue().equals(
                                vertex2.getVertexValue())) {
                    LOG.info("graph1[" + i
                            + "]'s id or value not equal to graph2[" + i
                            + "]'s");
                    return false;
                }
                List<Edge> edge1 = vertex1.getAllEdges();
                List<Edge> edge2 = vertex2.getAllEdges();
                if (edge1.size() != edge2.size()) {
                    LOG.info("graph1[" + i
                            + "]'s edgecount not equal to graph2[" + i + "]'s");
                    return false;
                }
                if (!vertex1.intoString().equals(vertex2.intoString())) {
                    LOG.info("graph1.getForAll(" + i
                            + ") not equal to graph2's");
                    return false;
                }
            }
            int activecount = ( int ) graph1.getActiveCounter();
            for (int i = 0; i < activecount; i++) {
                if (graph1.getActiveFlagForAll(i) != graph2
                        .getActiveFlagForAll(i)) {
                    LOG.info("graph1.getActiveFlagForAll(" + i
                            + ") not euql to graph2's");
                    return false;
                }
            }
        }
        return true;
    }

    public static void main(String[] args) throws Exception{
        try {
            BSPConfiguration conf = new BSPConfiguration();
            conf.addResource("testbcbsp-site.xml");
            FileSystem fs = FileSystem.get(conf);
            String datapath = "/usr/bcbsp_test/";
            boolean deleted = fs.delete(new Path(datapath, "checkpoint"), true);
            if (!deleted)
                throw new IOException();
        } catch (IOException e) {
            e.printStackTrace();
            LOG.warn("Delete the writepath("
                    + ") created by the testWriteCheckPoint() failed because of IOException:\t"
                    + e);
        }
    }
}