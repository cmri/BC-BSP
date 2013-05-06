/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.fault.storage;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

import com.chinamobile.bcbsp.fault.storage.DirRecord;

public class DirRecordTest {

    @Test
    public void testpushLocalFile() {
       File file1 = new File("/home/worker/input/forTest");
       DirRecord dRcord = new DirRecord();
       dRcord.pushLocalFile(file1);
       assertEquals(new File("/home/worker/input/forTest"),dRcord.getLocalFile(0));
       dRcord.deleteLocalFile(0);
       
    }
    public void testdeleteLocalFile(){
       File file1 = new File("/home/worker/input/ForTest");
       DirRecord dRcord = new DirRecord();
       dRcord.pushLocalFile(file1);
       dRcord.deleteLocalFile(0);
       assertEquals(null,dRcord.getLocalFile(0));
    }


}
