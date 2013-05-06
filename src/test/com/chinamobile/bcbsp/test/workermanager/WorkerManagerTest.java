/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.workermanager;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.sync.WorkerSSControllerInterface;
import com.chinamobile.bcbsp.test.TestUtil;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.workermanager.WorkerAgentForJob;
import com.chinamobile.bcbsp.workermanager.WorkerManager;

public class WorkerManagerTest extends TestCase {

    WorkerManager workerManager = null;
    BSPConfiguration conf = null;

    @Before
    protected void setUp() throws Exception {
        super.setUp();
        conf = new BSPConfiguration();
        conf.set(Constants.BC_BSP_CONTROLLER_ADDRESS, "127.0.0.1:40000");
        workerManager = new WorkerManager(conf);
    }

    @After
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testGetWorkerManagerNameBSPJobIDStaffAttemptID() {
        WorkerAgentForJob wafj = new WorkerAgentForJob(
                TestUtil.createDonothingObject(WorkerSSControllerInterface.class));
        Map<BSPJobID, WorkerAgentForJob> runningJobs = new ConcurrentHashMap<BSPJobID, WorkerAgentForJob>();
        try {
            TestUtil.set(workerManager, "runningJobtoWorkerAgent", runningJobs);
            TestUtil.set(wafj, "workerManagerName", "Test");
        } catch (Exception e) {
            e.printStackTrace();
        }
        BSPJobID jobID = new BSPJobID();
        StaffAttemptID staffID = new StaffAttemptID(jobID.getJtIdentifier(),
                jobID.getId(), 0, 0);
        runningJobs.put(jobID, wafj);
        String workerManagerName = workerManager.getWorkerManagerName(jobID,
                staffID);
        assertEquals(true, "Test".equals(workerManagerName));
    }

    @Test
    public void testGetNumberWorkers() {
        WorkerAgentForJob wafj = new WorkerAgentForJob(
                TestUtil.createDonothingObject(WorkerSSControllerInterface.class));
        BSPJobID jobID = new BSPJobID();
        StaffAttemptID staffID = new StaffAttemptID(jobID.getJtIdentifier(),
                jobID.getId(), 0, 0);
        wafj.setNumberWorkers(jobID, staffID, 2);
        Map<BSPJobID, WorkerAgentForJob> runningJobs = new ConcurrentHashMap<BSPJobID, WorkerAgentForJob>();
        runningJobs.put(jobID, wafj);
        try {
            TestUtil.set(workerManager, "runningJobtoWorkerAgent", runningJobs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        int num = workerManager.getNumberWorkers(jobID, staffID);
        assertEquals(2, num);
    }

    @Test
    public void testSetNumberWorkers() {
        WorkerAgentForJob wafj = new WorkerAgentForJob(
                TestUtil.createDonothingObject(WorkerSSControllerInterface.class));
        BSPJobID jobID = new BSPJobID();
        StaffAttemptID staffID = new StaffAttemptID(jobID.getJtIdentifier(),
                jobID.getId(), 0, 0);
        wafj.setNumberWorkers(jobID, staffID, 0);
        Map<BSPJobID, WorkerAgentForJob> runningJobs = new ConcurrentHashMap<BSPJobID, WorkerAgentForJob>();
        runningJobs.put(jobID, wafj);
        try {
            TestUtil.set(workerManager, "runningJobtoWorkerAgent", runningJobs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        int num = workerManager.getNumberWorkers(jobID, staffID);
        assertEquals(0, num);
        
        workerManager.setNumberWorkers(jobID, staffID, 2);
        num = workerManager.getNumberWorkers(jobID, staffID);
        assertEquals(2, num);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSetWorkerNametoPartitions() {
        WorkerAgentForJob wafj = new WorkerAgentForJob(
                TestUtil.createDonothingObject(WorkerSSControllerInterface.class));
        BSPJobID jobID = new BSPJobID();
        @SuppressWarnings("unused")
        StaffAttemptID staffID = new StaffAttemptID(jobID.getJtIdentifier(),
                jobID.getId(), 0, 0);
        Map<BSPJobID, WorkerAgentForJob> runningJobs = new ConcurrentHashMap<BSPJobID, WorkerAgentForJob>();
        runningJobs.put(jobID, wafj);
        HashMap<Integer, String> partitionToWorkerManagerName = null;
        int port = 0;
        try {
            partitionToWorkerManagerName = (HashMap<Integer, String>)TestUtil.get(wafj, "partitionToWorkerManagerName");
            TestUtil.set(workerManager, "runningJobtoWorkerAgent", runningJobs);
            port = (Integer)TestUtil.get(wafj, "portForJob");
        } catch (Exception e) {
            e.printStackTrace();
        }
        workerManager.setWorkerNametoPartitions(jobID, 5, "TEST");
        
        String check = partitionToWorkerManagerName.get(5);
        String shouldBe = "TEST:" + port;
        assertEquals(true, check.equals(shouldBe));
    }

    public static void main(String[] args) throws Exception {
    }
}
