/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.fault.browse;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.fault.browse.ReadFaultlog;
import com.chinamobile.bcbsp.fault.storage.Fault;

public class ReadFaultlogTest {

    ReadFaultlog readFaultlog;
    String localDirPath = "src/test/com/chinamobile/bcbsp/test/fault/browse/log";
    String[] keys = {"WORKERNODE","WARNING"};
    @Before
    public void setUp() throws Exception {

        String domainName = null;
        readFaultlog = new ReadFaultlog(localDirPath, domainName);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReadDir() {
        System.out.println("----------------testReadDir-----------------");
        List<Fault> listReadDir = readFaultlog.readDir(localDirPath);
        for (Iterator iterator = listReadDir.iterator(); iterator.hasNext();) {
            Fault fault = ( Fault ) iterator.next();
            System.out.println(fault);
        }
        assertEquals(12, listReadDir.size());
    }
   
    @SuppressWarnings("unchecked")
    @Test
    public void testReadDirWithKey() {
        System.out.println();
        System.out.println("----------------testReadDirWithKey----------------");
        List<Fault> listReadDir = readFaultlog.readDirWithKey(localDirPath, keys);
        for (Iterator iterator = listReadDir.iterator(); iterator.hasNext();) {
            Fault fault = ( Fault ) iterator.next();
            System.out.println(fault);
        }
        assertEquals(3, listReadDir.size());
    }

}
