/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.fault.browse;

import org.junit.Ignore;
import org.junit.Test;
import com.chinamobile.bcbsp.fault.browse.MessageService;
import com.chinamobile.bcbsp.fault.storage.Fault;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import com.chinamobile.bcbsp.TestUtil;

public class MessageServiceTest extends TestCase{
    MessageService ms ;
    List<Fault> testresult;
    public void setUp() throws IOException, InterruptedException {
        ms = new MessageService();
        ms.setCurrentPage(1);
        ms.setPageSize(10);
        testresult = new ArrayList<Fault>();
     
    try{
        //TestUtil.set(ms, "totalList", testresult);
        TestUtil.invoke(ms,"setTotalList",testresult);
        TestUtil.invoke(ms,"getTotalList",testresult);
        
            
        } catch(Exception e){
            //
        }
    }
    

    @Test
    public void testGetCurrentPage() {
        
        assertEquals("testGetCurrentPage()",1,ms.getCurrentPage());
    }

    @Test
    public void testGetPageSize() {
        
        assertEquals("testGetPageSize()",10,ms.getPageSize());
   
    }

    @Test
    @Ignore("GetTotalRows(): to be tested")
    public void testGetTotalRows() throws Exception {
        
        assertEquals("testGetTotalRows",testresult.size(),ms.getTotalRows()); 
    }

    @Test
    @Ignore("GetTotalPages(): to be tested")
    public void testGetTotalPages() {
        
    }

    @Test
    @Ignore("GetPageByType(): to be tested")
    public void testGetPageByType() throws Exception{
    }

    @Test
    @Ignore("GetPageByTypeIntStringInt(): to be tested")
    public void testGetPageByTypeIntStringInt() {

    }

    @Test
    @Ignore("GetPageByKeyIntString(): to be tested")
    public void testGetPageByKeyIntString() {
       
    }

    @Test
    @Ignore("GetPageByKeyIntStringInt(): to be tested")
    public void testGetPageByKeyIntStringInt() {
       
    }

    @Test
    @Ignore("GetPageByKeysIntStringArray(): to be tested")
    public void testGetPageByKeysIntStringArray() {
       
    }

    @Test
    @Ignore("GetPageByKeysIntStringArrayInt(): to be tested")
    public void testGetPageByKeysIntStringArrayInt() {
        
    }

}
