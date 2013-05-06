/**
 * CopyRight by Chinamobile
 * 
 * GraphDataFactory.java
 */
package com.chinamobile.bcbsp.graph;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.bspstaff.Staff;

public class GraphDataFactory {
    private static final Log LOG = LogFactory.getLog(GraphDataFactory.class);
    // the subscript of version2GraphData is the version
    private  Class<?>[] version2GraphData = null;
    private static Class<?>[] defaultClasses = {GraphDataForMem.class, GraphDataForDisk.class, GraphDataForBDB.class};
    
    public GraphDataFactory(Configuration conf){
        getGraphDataClasses(conf);
    }
    
    private void getGraphDataClasses(Configuration conf) {
        version2GraphData = conf.getClasses(Constants.USER_BC_BSP_JOB_GRAPHDATA_CLASS);
        if( version2GraphData == null || version2GraphData.length == 0 ){
            version2GraphData = defaultClasses;
        }
    }

    @SuppressWarnings("unchecked")
    public GraphDataInterface createGraphData(int version, Staff staff) {
        Class graphDataClass = version2GraphData[version];
        GraphDataInterface graphData = null;
        try {
            graphData = ( GraphDataInterface ) graphDataClass.newInstance();
        } catch (Exception e) {
            LOG.warn("Can not find the class:" + version2GraphData[version]);
            LOG.warn("Use the default GraphDataForMem");
            graphData = new GraphDataForMem();
        }

        graphData.setStaff(staff);
        graphData.initialize();

        return graphData;
    }
}
