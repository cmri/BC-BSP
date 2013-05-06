/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.fault.tools;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.fault.storage.DirRecord;
import com.chinamobile.bcbsp.fault.tools.GetRecord;

public class GetRecordTest {
    GetRecord getRecord;
    DirRecord dr;
    String hdfsFile = "/user/faultlog/DirRecord.bak";

    @Before
    public void setUp() throws Exception {
        String localRecordPath = "test/com/chinamobile/bcbsp/fault/tools/DirRecord.bak";
        getRecord = new GetRecord(localRecordPath, null);
        System.out.println(getRecord);

    }

    @Test
    public void testGetLocalFilePath() {
        String s = GetRecord.getLocalFilePath(hdfsFile);
        assertEquals("/DirRecord.bak", s);
        System.out.println(s);

    }

}
