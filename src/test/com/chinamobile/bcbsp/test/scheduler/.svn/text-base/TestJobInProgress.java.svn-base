/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.scheduler;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.bspcontroller.BSPController;
import com.chinamobile.bcbsp.bspcontroller.JobInProgress;
import com.chinamobile.bcbsp.bspstaff.StaffInProgress;
import com.chinamobile.bcbsp.test.mini.MiniBSPController;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffStatus;
import com.chinamobile.bcbsp.workermanager.WorkerManagerStatus;

import junit.framework.TestCase;

public class TestJobInProgress extends TestCase {
    
    private MiniBSPController master;
    private JobInProgress jip;
    
    private String jobId = "job_201207241653_0001";
    private int staffNum;
    
    private HashMap<Integer, String[]> locations = new HashMap<Integer, String[]>();
    
    @Before
    public void setUp() throws Exception {
        String[] location_one = {"worker_one", "worker_two", "worker_three"};
        this.locations.put(0, location_one);
        
        this.staffNum = locations.size();
        BSPConfiguration conf = new BSPConfiguration();
        conf.set(Constants.BC_BSP_CONTROLLER_ADDRESS, "localhost:40000");
        conf.set(Constants.BC_BSP_SHARE_DIRECTORY, "${hadoop.tmp.dir}/bcbsp/share");
        this.master = new MiniBSPController(conf);
        this.jip = new JobInProgress(new BSPJob(conf), new BSPJobID().forName(this.jobId), (BSPController)this.master,
                this.staffNum, this.locations);
    }

    @After
    public void tearDown() throws Exception {
        //this.master.stopServer();
    }
    
    /**
     * Test the data location.
     * We set the situation that the worker to excute the staff-0 is worker_one.
     */
    @Test
    public void testDataLocation() {
        List<StaffStatus> reports = new ArrayList<StaffStatus>();
        WorkerManagerStatus wms_one = new WorkerManagerStatus("worker_one", reports, 5, 1, 2, 0);
        WorkerManagerStatus wms_two = new WorkerManagerStatus("worker_two", reports, 5, 3, 2, 0);
        WorkerManagerStatus wms_three = new WorkerManagerStatus("worker_three", reports, 5, 3, 2, 0);
        WorkerManagerStatus wms_four = new WorkerManagerStatus("worker_four", reports, 5, 0, 2, 0);
        WorkerManagerStatus[] wmss = new WorkerManagerStatus[]{wms_one, wms_two, wms_three, wms_four};
        
        int maxStaffNum = 0, runningStaffNum = 0, remainStaffNum = 0;
        maxStaffNum += wms_one.getMaxStaffsCount(); runningStaffNum += wms_one.getRunningStaffsCount();
        maxStaffNum += wms_two.getMaxStaffsCount(); runningStaffNum += wms_two.getRunningStaffsCount();
        maxStaffNum += wms_three.getMaxStaffsCount(); runningStaffNum += wms_three.getRunningStaffsCount();
        maxStaffNum += wms_four.getMaxStaffsCount(); runningStaffNum += wms_four.getRunningStaffsCount();
        remainStaffNum = this.jip.getNumBspStaff();
        
        float taskLoadFactor = (float)(remainStaffNum + runningStaffNum) / maxStaffNum;
        
        //System.out.println("MaxStaffNum: " + maxStaffNum);
        //System.out.println("RunningStaffNum: " + runningStaffNum);
        //System.out.println("RemainStaffNum: " + remainStaffNum);
        //System.out.println("TaskLoadFactor: " + taskLoadFactor);
        
        StaffInProgress[] sips = this.jip.getStaffInProgress();
        for (int i = 0; i < sips.length; i++) {
            this.jip.obtainNewStaff(wmss, 0, taskLoadFactor);
            assertEquals(wms_one.getWorkerManagerName(), sips[0].getWorkerManagerStatus().getWorkerManagerName());
        }
    }
    
    /**
     * Test the data location Exception: the HDFS cluster includes the BSP cluster.
     * We set the situation that the worker to excute the staff-0 is worker_two.
     */
    @Test
    public void testException() {
        List<StaffStatus> reports = new ArrayList<StaffStatus>();
        WorkerManagerStatus wms_one = new WorkerManagerStatus("worker_one", reports, 5, 4, 2, 0);
        WorkerManagerStatus wms_two = new WorkerManagerStatus("worker_two", reports, 5, 3, 2, 0);
        WorkerManagerStatus[] wmss = new WorkerManagerStatus[]{wms_one, wms_two};
        
        int maxStaffNum = 0, runningStaffNum = 0, remainStaffNum = 0;
        maxStaffNum += wms_one.getMaxStaffsCount(); runningStaffNum += wms_one.getRunningStaffsCount();
        maxStaffNum += wms_two.getMaxStaffsCount(); runningStaffNum += wms_two.getRunningStaffsCount();
        remainStaffNum = this.jip.getNumBspStaff();
        
        float taskLoadFactor = (float)(remainStaffNum + runningStaffNum) / maxStaffNum;
        
        //System.out.println("MaxStaffNum: " + maxStaffNum);
        //System.out.println("RunningStaffNum: " + runningStaffNum);
        //System.out.println("RemainStaffNum: " + remainStaffNum);
        //System.out.println("TaskLoadFactor: " + taskLoadFactor);
        
        StaffInProgress[] sips = this.jip.getStaffInProgress();
        for (int i = 0; i < sips.length; i++) {
            this.jip.obtainNewStaff(wmss, 0, taskLoadFactor);
            assertEquals(wms_two.getWorkerManagerName(), sips[0].getWorkerManagerStatus().getWorkerManagerName());
        }
    }
    
    /**
     * Test the fair load function.
     * We set the situation that the worker to excute the staff-0 is worker_four.
     */
    @Test
    public void testFairLoad() {
        List<StaffStatus> reports = new ArrayList<StaffStatus>();
        WorkerManagerStatus wms_one = new WorkerManagerStatus("worker_one", reports, 5, 3, 2, 0);
        WorkerManagerStatus wms_two = new WorkerManagerStatus("worker_two", reports, 5, 3, 2, 0);
        WorkerManagerStatus wms_three = new WorkerManagerStatus("worker_three", reports, 5, 3, 2, 0);
        WorkerManagerStatus wms_four = new WorkerManagerStatus("worker_four", reports, 5, 0, 2, 0);
        WorkerManagerStatus[] wmss = new WorkerManagerStatus[]{wms_one, wms_two, wms_three, wms_four};
        
        int maxStaffNum = 0, runningStaffNum = 0, remainStaffNum = 0;
        maxStaffNum += wms_one.getMaxStaffsCount(); runningStaffNum += wms_one.getRunningStaffsCount();
        maxStaffNum += wms_two.getMaxStaffsCount(); runningStaffNum += wms_two.getRunningStaffsCount();
        maxStaffNum += wms_three.getMaxStaffsCount(); runningStaffNum += wms_three.getRunningStaffsCount();
        maxStaffNum += wms_four.getMaxStaffsCount(); runningStaffNum += wms_four.getRunningStaffsCount();
        remainStaffNum = this.jip.getNumBspStaff();
        
        float taskLoadFactor = (float)(remainStaffNum + runningStaffNum) / maxStaffNum;
        
        //System.out.println("MaxStaffNum: " + maxStaffNum);
        //System.out.println("RunningStaffNum: " + runningStaffNum);
        //System.out.println("RemainStaffNum: " + remainStaffNum);
        //System.out.println("TaskLoadFactor: " + taskLoadFactor);
        
        StaffInProgress[] sips = this.jip.getStaffInProgress();
        for (int i = 0; i < sips.length; i++) {
            this.jip.obtainNewStaff(wmss, 0, taskLoadFactor);
            assertEquals(wms_four.getWorkerManagerName(), sips[0].getWorkerManagerStatus().getWorkerManagerName());
        }
    }
}
