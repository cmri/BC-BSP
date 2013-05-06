/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.client;
import java.io.IOException;

import com.chinamobile.bcbsp.util.*;
import com.chinamobile.bcbsp.client.RunningJob;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.Ignore;

import com.chinamobile.bcbsp.util.JobStatus;
import com.chinamobile.bcbsp.client.BSPJobClient;

/**
 * BSPJobClientTest
 * 
 * Test the BSPJobClient.
 * 
 * @author MAYUE
 * @version
 */

public class BSPJobClientTest {

    @Test
    @Ignore("ReadFields(): to be tested")
    public void testBSPJobClientConfiguration() {
        //fail("Not yet implemented");
    }

    @Test
    @Ignore("ReadFields(): to be tested")
    public void testBSPJobClient() {
       // fail("Not yet implemented");
    }

    @Test
    @Ignore("ReadFields(): to be tested")
    public void testInit() {
       // fail("Not yet implemented");
    }

    @Test
    @Ignore("ReadFields(): to be tested")
    public void testClose() {
        //fail("Not yet implemented");
    }

    @Test
    @Ignore("ReadFields(): to be tested")
    public void testGetFs() {
       // fail("Not yet implemented");
  
    }

    @Test
    @Ignore("ReadFields(): to be tested")
    public void testGetAllJobs() {
        //fail("Not yet implemented");
    }

    @Test
    @Ignore("ReadFields(): to be tested")
    public void testGetJobSubmitClient() {
        //fail("Not yet implemented");
    }

    @Test
    @Ignore("ReadFields(): to be tested")
    public void testJobsToComplete() {
        //fail("Not yet implemented");
    }

    @Test
    @Ignore("ReadFields(): to be tested")
    public void testSubmitJob() {
        //fail("Not yet implemented");
    }

    @Test
    @Ignore("ReadFields(): to be tested")
    public void testSubmitJobInternal() {
        //fail("Not yet implemented");
    }

    @Test
    @Ignore("MonitorAndPrintJob(): to be test")
    public void testMonitorAndPrintJob() throws IOException, InterruptedException {
        
        // test object
        BSPJobClient testbspjobclient = new BSPJobClient();
       
        //param1 job
        RunningJob info;
        
        JobStatus testJobStatus1;
        String strIden1 = new String("testID1");
        BSPJobID bspJobID1 = new BSPJobID(strIden1,1);
        String strUser1 = new String("MaYue1");
        int nProgress1 = 10;
        int nRunState1 = JobStatus.SUCCEEDED;
        testJobStatus1 = new JobStatus(bspJobID1,strUser1,nProgress1,nRunState1);
        info = testbspjobclient.new NetworkedJob(testJobStatus1);
        
        //param2 info
        BSPJobTestClass job = new BSPJobTestClass();
        
        boolean testResult = testbspjobclient.monitorAndPrintJob(job,info);
        boolean expect = true;
        
        assertEquals("the job is successful",expect,testResult);

    }

    @Test
    @Ignore("ReadFields(): to be tested")
    public void testGetSystemDir() {
        //fail("Not yet implemented");
    }

    @Test
    @Ignore("ReadFields(): to be tested")
    public void testRunJob()   {
      //fail("Not yet implemented");
    }

    @Test
    @Ignore("ReadFields(): to be tested")
    public void testGetClusterStatus(){
        //fail("Not yet implemented");
    }
    

    @Test
    public void testRun() throws Exception {
        
        int nExpectedResult = -1;
        BSPJobClient testBSPJobClient = new BSPJobClient();
       
        System.out.println("Test cmd is NULL");
        String[] NullCmd = new String[]{};
        int nTestResult;
        
        try {
            nTestResult = testBSPJobClient.run(NullCmd);
            assertEquals("Expected result is -1",nExpectedResult,nTestResult);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        System.out.println("Test cmd is -list");
        String[] listCmd = new String[]{new String("-list")};
        
        try {
            nTestResult = testBSPJobClient.run(listCmd);
            assertEquals("Expected result is -1",nExpectedResult,nTestResult);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        System.out.println("Test cmd is -workers");
        String[] workersCmd = new String[]{new String("-workers")};
              
        try {
            nTestResult = testBSPJobClient.run(workersCmd);
            assertEquals("Expected result is -1",nExpectedResult,nTestResult);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        System.out.println("Test cmd is -submit");
        String[] submitCmd = new String[]{new String("-submit")};
       
        try {
            nTestResult = testBSPJobClient.run(submitCmd);
            assertEquals("Expected result is -1",nExpectedResult,nTestResult);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        System.out.println("Test cmd is -kill");
        String[] killCmd = new String[]{new String("-kill")};
              
        try {
            nTestResult = testBSPJobClient.run(killCmd);
            assertEquals("Expected result is -1",nExpectedResult,nTestResult);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        System.out.println("Test cmd is -status");
        String[] statusCmd = new String[]{new String("-status")};        
       
        try {
            nTestResult = testBSPJobClient.run(statusCmd);
            assertEquals("Expected result is -1",nExpectedResult,nTestResult);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        System.out.println("Test cmd is -list-staffs");
        String[] list_staffsCmd = new String[]{new String("-list-staffs")};       
       
        try {
            nTestResult = testBSPJobClient.run(list_staffsCmd);
            assertEquals("Expected result is -1",nExpectedResult,nTestResult);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        System.out.println("Test cmd is -setcheckpoint");
        String[] setcheckpointCmd = new String[]{new String("-setcheckpoint")};
               
        try {
            nTestResult = testBSPJobClient.run(setcheckpointCmd);
            assertEquals("Expected result is -1",nExpectedResult,nTestResult);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        
        System.out.println("Test cmd is -master");
        String[] masterCmd = new String[]{new String("-master")};

        try {
            nTestResult = testBSPJobClient.run(masterCmd);
            
            assertEquals("Expected result is -1",nExpectedResult,nTestResult);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        System.out.println("Test cmd is -kill-task");
        String[] kill_taskCmd = new String[]{new String("-kill-task")};
              
        try {
            nTestResult = testBSPJobClient.run(kill_taskCmd);
            
            assertEquals("Expected result is -1",nExpectedResult,nTestResult);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        System.out.println("Test cmd is -fail-task");
        String[] fail_taskCmd = new String[]{new String("-fail-task")};
               
        try {
            nTestResult = testBSPJobClient.run(fail_taskCmd);
            
            assertEquals("Expected result is -1",nExpectedResult,nTestResult);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
 }

    @Test
    public void testDisplayJobList() {
        
        JobStatus testJobStatus1; //RUNNING
        JobStatus testJobStatus2; //KILLED
        JobStatus testJobStatus3; //SUCCEEDED
        JobStatus testJobStatus4; //FAILED
        JobStatus testJobStatus5; //PREP
        JobStatus testJobStatus6; //RECOVERY
        
        JobStatus[] testJobStatusArray;
        
        String strIden1 = new String("testID1");
        BSPJobID bspJobID1 = new BSPJobID(strIden1,1);
        String strUser1 = new String("MaYue1");
        int nProgress1 = 10;
        int nRunState1 = JobStatus.RUNNING;
        testJobStatus1 = new JobStatus(bspJobID1,strUser1,nProgress1,nRunState1);
        
        String strIden2 = new String("testID2");
        BSPJobID bspJobID2 = new BSPJobID(strIden2,2);
        String strUser2 = new String("MaYue2");
        int nProgress2 = 80;
        int nRunState2 = JobStatus.KILLED;
        testJobStatus2 = new JobStatus(bspJobID2,strUser2,nProgress2,nRunState2);
        
        String strIden3 = new String("testID3");
        BSPJobID bspJobID3 = new BSPJobID(strIden3,3);
        String strUser3 = new String("MaYue3");
        int nProgress3 = 20;
        int nRunState3 = JobStatus.SUCCEEDED;
        testJobStatus3 = new JobStatus(bspJobID3,strUser3,nProgress3,nRunState3);
        
        String strIden4 = new String("testID4");
        BSPJobID bspJobID4 = new BSPJobID(strIden4,4);
        String strUser4 = new String("MaYue4");
        int nProgress4 = 30;
        int nRunState4 = JobStatus.FAILED;
        testJobStatus4 = new JobStatus(bspJobID4,strUser4,nProgress4,nRunState4);
        
        String strIden5 = new String("testID5");
        BSPJobID bspJobID5 = new BSPJobID(strIden5,5);
        String strUser5 = new String("MaYue5");
        int nProgress5 = 40;
        int nRunState5 = JobStatus.PREP;
        testJobStatus5 = new JobStatus(bspJobID5,strUser5,nProgress5,nRunState5);
        
        String strIden6 = new String("testID6");
        BSPJobID bspJobID6 = new BSPJobID(strIden6,6);
        String strUser6 = new String("MaYue6");
        int nProgress6 = 50;
        int nRunState6 = JobStatus.RECOVERY;
        testJobStatus6 = new JobStatus(bspJobID6,strUser6,nProgress6,nRunState6);
        
        
        
        testJobStatusArray = new JobStatus[]{testJobStatus1,testJobStatus2,testJobStatus3,
                testJobStatus4,testJobStatus5,testJobStatus6};
        BSPJobClient testBSPJobClient = new BSPJobClient();
        testBSPJobClient.displayJobList(testJobStatusArray);
        
        //test boundary
        JobStatus[] EmptyJobStatusArray = new JobStatus[]{};
        testBSPJobClient.displayJobList(EmptyJobStatusArray);
        
        //JobStatus[] EmptyContentJobStatusArray = new JobStatus[10];
        //testBSPJobClient.displayJobList(EmptyContentJobStatusArray);

}
    

    @Test
    @Ignore("ReadFields(): to be tested")
    public void testMain() {
        //fail("Not yet implemented");
    }
}
