/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.fault.browse;

import java.io.IOException;

import junit.framework.TestCase;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.fault.browse.PageService;

import org.junit.Test;

public class PageServiceTest extends TestCase {
    PageService pService;

    public void setUp() throws IOException, InterruptedException {

        pService = new PageService();
        pService.setMonth("Monday");
        pService.setKey("bsp");
        pService.setLevel(Constants.PRIORITY.HIGHER);// "1"
        pService.setPageNumber(20);
        pService.setTime("20121023");
        pService.setType(Constants.PARTITION_TYPE.HASH);
        pService.setWorker("1");
    }

    @Test
    public void testGetMonth() {
        assertEquals("Monday", pService.getMonth());
    }

    @Test
    public void testGetLevel() {
        assertEquals("1", pService.getLevel());
    }

    @Test
    public void testGetTime() {
        assertEquals("20121023", pService.getTime());
    }

    @Test
    public void testGetWorker() {
        assertEquals("1", pService.getWorker());
    }

    @Test
    public void testGetKey() {
        assertEquals("bsp", pService.getKey());
    }

    @Test
    public void testGetType() {
        assertEquals("hash", pService.getType());
    }

    @Test
    public void testGetPageNumber() {
        assertEquals(20, pService.getPageNumber());
    }

    @Test
    public void testGetFirstPage() {
        StringBuffer html = new StringBuffer();

        html.append("<font size = '3' >");
        html.append("<table frame = 'box'  >");
        html.append("<tr><td width='59' bgcolor='#78A5D1'>TIME</td><td width='59' bgcolor='#78A5D1'>TYPE</td><td width='59' bgcolor='#78A5D1'>LEVEL</td><td width='59' bgcolor='#78A5D1'>WORKER</td>");
        html.append("<td width='59' bgcolor='#78A5D1'>JOBNAME</td><td width='59' bgcolor='#78A5D1'>STAFFNAME</td><td width='59' bgcolor='#78A5D1'>STATUS</td><td width='59' bgcolor='#78A5D1'>EXCEPTIONMESSAGE</td></tr>");
        html.append("</table>");
        html.append("</font>");
        String expect = html.toString();
        assertEquals(expect, pService.getFirstPage());
    }

}
