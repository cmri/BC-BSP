/**
 * CopyRight by Chinamobile
 * 
 * Checkpoint.java
 */
package com.chinamobile.bcbsp.fault.storage;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.graph.GraphDataFactory;
import com.chinamobile.bcbsp.graph.GraphDataInterface;
import com.chinamobile.bcbsp.io.OutputFormat;
import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.util.BSPJob;

public class Checkpoint {
    
    private static final Log LOG = LogFactory.getLog(Checkpoint.class);
    
    private Class<? extends Vertex<?,?,?>> vertexClass;
    
    public Checkpoint(BSPJob job) {
        vertexClass = job.getVertexClass();
    }
    
    /**
     * Write Check point
     * 
     * @param writePath
     * @param job
     * @param staff
     * @return boolean
     */
    @SuppressWarnings("unchecked")
    public boolean writeCheckPoint(GraphDataInterface graphData, Path writePath, BSPJob job, Staff staff) throws IOException {
        LOG.info("The init write path is : " + writePath.toString());
        try {
            OutputFormat outputformat = ( OutputFormat ) ReflectionUtils.newInstance(job.getConf().getClass(Constants.USER_BC_BSP_JOB_OUTPUT_FORMAT_CLASS, OutputFormat.class), job.getConf());
            outputformat.initialize(job.getConf());
         
            RecordWriter output = outputformat.getRecordWriter(job, staff.getStaffAttemptId(), writePath);
            for (int i = 0; i < graphData.sizeForAll(); i++) {
                Vertex<?,?,Edge> vertex = graphData.getForAll(i);
                StringBuffer outEdges = new StringBuffer();
                for (Edge edge : vertex.getAllEdges()) {
                    outEdges.append(edge.getVertexID() + Constants.SPLIT_FLAG + edge.getEdgeValue() + Constants.SPACE_SPLIT_FLAG);
                }
                if (outEdges.length() > 0) {
                    int j = outEdges.length();
                    outEdges.delete(j - 1, j - 1);
                }
                output.write(new Text(vertex.getVertexID() + Constants.SPLIT_FLAG + vertex.getVertexValue()), new Text(outEdges.toString()));
            }
            output.close(job);
        } catch (Exception e) {
            LOG.error("Exception has happened and been catched!", e);
            return false;
        }
        return true;
    }
    
    /**
     * Read Check point
     * 
     * @param readPath
     * @param job
     * @return
     */
    @SuppressWarnings("unchecked")
    public GraphDataInterface readCheckPoint(Path readPath, BSPJob job, Staff staff) {

        LOG.info("The init read path is : "
                + readPath.toString());

        String uri = readPath.toString() + "/" + staff.getStaffAttemptId().toString() + "/checkpoint.cp";
        LOG.info("uri: " + uri);
        Configuration conf = new Configuration();
        InputStream in = null;
        BufferedReader bis = null;
        
        GraphDataInterface graphData = null;
        GraphDataFactory graphDataFactory = staff.getGraphDataFactory();
        int version = job.getGraphDataVersion();
        graphData = graphDataFactory.createGraphData(version, staff);

        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);    
            in = fs.open(new Path(uri));
            bis = new BufferedReader(new InputStreamReader(
                    new BufferedInputStream(in)));
            Vertex vertexTmp = null;
            String s = bis.readLine();

            while (s != null) {
                try {
                    vertexTmp = this.vertexClass.newInstance();
                    vertexTmp.fromString(s);
                } catch (Exception e) {
                    LOG.error("[Checkpoint] caught: ", e);
                }
                
                if (vertexTmp != null) {
                    graphData.addForAll(vertexTmp);
                } 
                s = bis.readLine();
                
            }
            graphData.finishAdd();

            bis.close();
        } catch (IOException e) {
            LOG.error("Exception has happened and been catched!", e);
        }

        return graphData;
    }
}
