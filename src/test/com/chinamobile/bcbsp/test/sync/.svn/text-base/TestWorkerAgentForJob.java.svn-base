/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.sync;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.workermanager.WorkerAgentForJob;

public class TestWorkerAgentForJob {
    
    private MiniWorkerSSController wssc;
    private int localBarrierNum;
    
    private String jobId;
    private int superStepCounter;
    
    @Before
    public void setUp() throws Exception {
        this.wssc = new MiniWorkerSSController();
        this.localBarrierNum = 2;
        this.jobId = "job_201207241653_0001";
        this.superStepCounter = 2;
    }

    @After
    public void tearDown() throws Exception {
        
    }
    
    @SuppressWarnings("static-access")
    @Test
    public void testLocalBarrierFirstStage() {
        SuperStepReportContainer ssrc_one = new SuperStepReportContainer();
        ssrc_one.setLocalBarrierNum(this.localBarrierNum);
        ssrc_one.setStageFlag(Constants.SUPERSTEP_STAGE.FIRST_STAGE);//
        ssrc_one.setDirFlag(new String[] { "1" });
        ssrc_one.setCheckNum(0);
        
        SuperStepReportContainer ssrc_two = new SuperStepReportContainer();
        ssrc_two.setLocalBarrierNum(this.localBarrierNum);
        ssrc_two.setStageFlag(Constants.SUPERSTEP_STAGE.FIRST_STAGE);//
        ssrc_two.setDirFlag(new String[] { "1" });
        ssrc_two.setCheckNum(0);
        
        WorkerAgentForJob workerAgent = new WorkerAgentForJob(this.wssc);
        
        String staffId_one = "attempt_201207241653_0001_000001_0";
        boolean result_one = workerAgent.localBarrier(new BSPJobID().forName(this.jobId), new StaffAttemptID().forName(staffId_one),
                this.superStepCounter, ssrc_one);
        assertEquals(false, result_one);
        
        String staffId_two = "attempt_201207241653_0001_000002_0";
        boolean result_two = workerAgent.localBarrier(new BSPJobID().forName(this.jobId), new StaffAttemptID().forName(staffId_two),
                this.superStepCounter, ssrc_one);
        assertEquals(true, result_two);
    }
    
    @SuppressWarnings("static-access")
    @Test
    public void testLocalBarrierSecondStage() {
        SuperStepReportContainer ssrc_one = new SuperStepReportContainer();
        ssrc_one.setLocalBarrierNum(this.localBarrierNum);
        ssrc_one.setStageFlag(Constants.SUPERSTEP_STAGE.SECOND_STAGE);//
        ssrc_one.setDirFlag(new String[] { "1" });
        ssrc_one.setCheckNum(0);
        
        SuperStepReportContainer ssrc_two = new SuperStepReportContainer();
        ssrc_two.setLocalBarrierNum(this.localBarrierNum);
        ssrc_two.setStageFlag(Constants.SUPERSTEP_STAGE.SECOND_STAGE);//
        ssrc_two.setDirFlag(new String[] { "1" });
        ssrc_two.setCheckNum(0);
        
        WorkerAgentForJob workerAgent = new WorkerAgentForJob(this.wssc);
        
        String staffId_one = "attempt_201207241653_0001_000001_0";
        boolean result_one = workerAgent.localBarrier(new BSPJobID().forName(this.jobId), new StaffAttemptID().forName(staffId_one),
                this.superStepCounter, ssrc_one);
        assertEquals(false, result_one);
        
        String staffId_two = "attempt_201207241653_0001_000002_0";
        boolean result_two = workerAgent.localBarrier(new BSPJobID().forName(this.jobId), new StaffAttemptID().forName(staffId_two),
                this.superStepCounter, ssrc_one);
        assertEquals(true, result_two);
    }
}
