/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.bspstaff;

import java.util.HashMap;
import java.util.Map.Entry;
import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;

public class TestBSPStaff extends TestCase {
    
    private BSPStaff staff;
    
    private static int NumCopy = 2;
    private static int FailCounter = 3;
    private HashMap<Integer, String> route = new HashMap<Integer, String>();
    private HashMap<Integer, Integer> table = new HashMap<Integer, Integer>();
    
    @Before
    public void setUp() throws Exception {
        staff = new BSPStaff();
        staff.setNumCopy(NumCopy);
        staff.setFailCounter(FailCounter);
        
        route.put(1, "worker_one:61000");
        route.put(2, "worker_one:61001");
        route.put(3, "worker_two:61000");
        route.put(4, "worker_three:61000");
        staff.setPartitionToWorkerManagerNameAndPort(route);
        
        table.put(1, 1);
        table.put(2, 1);
        staff.setHashBucketToPartition(table);
    }
    
    @Test
    public void testGetNumCopy() {
        assertEquals(staff.getNumCopy(), NumCopy);
    }
    
    @Test
    public void testGetFailCounter() {
        assertEquals(staff.getFailCounter(), FailCounter);
    }
    
    @Test
    public void testGetPartitionToWorkerManagerNameAndPort() {
        HashMap<Integer, String> testRoute = staff.getPartitionToWorkerManagerNameAndPort();
        for (Entry<Integer, String> entry: testRoute.entrySet()) {
            assertEquals(entry.getValue(), route.get(entry.getKey()));
        }
    }
    
    @Test
    public void testGetLocalBarrierNumber() {
        assertEquals(staff.getLocalBarrierNumber("worker_one"), 2);
    }
    
    @Test
    public void testGetHashBucketToPartition() {
        HashMap<Integer, Integer> testTable = staff.getHashBucketToPartition();
        for (Entry<Integer, Integer> entry: testTable.entrySet()) {
            assertEquals(entry.getValue(), table.get(entry.getKey()));
        }
    }
}
