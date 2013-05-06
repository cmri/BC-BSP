/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.aggregate;

import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.bspcontroller.BSPController;
import com.chinamobile.bcbsp.bspcontroller.JobInProgress;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;
import com.chinamobile.bcbsp.test.mini.MiniBSPController;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

import junit.framework.TestCase;

public class TestJobInProgress extends TestCase {
    
    private MiniBSPController master;
    private JobInProgress jip;
    
    private String jobId = "job_201207241653_0001";
    private int staffNum;
    private String aggName = "errorSum";
    
    private HashMap<Integer, String[]> locations = new HashMap<Integer, String[]>();
    
    @Before
    public void setUp() throws Exception {
        this.staffNum = locations.size();
        BSPConfiguration conf = new BSPConfiguration();
        conf.set(Constants.BC_BSP_CONTROLLER_ADDRESS, "localhost:40000");
        conf.set(Constants.BC_BSP_SHARE_DIRECTORY, "${hadoop.tmp.dir}/bcbsp/share");
        this.master = new MiniBSPController(conf);
        BSPJob job = new BSPJob(conf);
        job.registerAggregator(this.aggName, ErrorSumAggregator.class,
                ErrorAggregateValue.class);
        job.completeAggregatorRegister();
        this.jip = new JobInProgress(job, new BSPJobID().forName(this.jobId), (BSPController)this.master,
                this.staffNum, this.locations);
    }

    @After
    public void tearDown() throws Exception {
        //this.master.stopServer();
    }
    
    @Test
    public void testGeneralAggregate() {
        SuperStepReportContainer ssrc_one = new SuperStepReportContainer(Constants.SUPERSTEP_STAGE.FIRST_STAGE, 
                new String[] { "1" }, 1L, new String[]{this.aggName + Constants.KV_SPLIT_FLAG + "12.344"});
        
        SuperStepReportContainer ssrc_two = new SuperStepReportContainer(Constants.SUPERSTEP_STAGE.FIRST_STAGE, 
                new String[] { "1" }, 1L, new String[]{this.aggName + Constants.KV_SPLIT_FLAG + "11.111"});
        
        SuperStepReportContainer[] ssrcs = new SuperStepReportContainer[]{ssrc_one, ssrc_two};
        
        String[] result = this.jip.generalAggregate(ssrcs);
        
        assertEquals(23.455f, Float.valueOf(result[0].split(Constants.KV_SPLIT_FLAG)[1]));
    }
}
