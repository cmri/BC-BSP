/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.client;

import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;

import com.chinamobile.bcbsp.client.BSPJobClientHelp;
/**
 * BSPJobClientHelpTest
 * 
 * Test the BSPJobClientHelp.
 * 
 * @author MAYUE
 * @version
 */

public class BSPJobClientHelpTest {
    
    String [] cmd1 = new String[]{};
    
    String cmd2 = "jar";
    String cmd3 = "job";
    String cmd4 = "job -list";
    String cmd5 = "job -kill";
    String cmd6 = "job -list-staffs";
    String cmd7 = "job -setcheckpoint";
    String cmd8 = "admin";
    String cmd9 = "admin -master";
    String cmda = "admin -workers";
    String cmdb = "COMMAND LIST";
    
   
    @Test
    @Ignore("ReadFields(): to be tested")
    public void testSetConf() {
       
    }

    @Test
    @Ignore("ReadFields(): to be tested")
    public void testGetConf() {
        
    }

    @Test
    public void testRun() throws Exception{
        
        String [] cmd21 = cmd2.split(" ");
        String [] cmd31 = cmd3.split(" ");
        String [] cmd41 = cmd4.split(" ");
        String [] cmd51 = cmd5.split(" ");
        String [] cmd61 = cmd6.split(" ");
        String [] cmd71 = cmd7.split(" ");
        String [] cmd81 = cmd8.split(" ");
        String [] cmd91 = cmd9.split(" ");
        String [] cmda1 = cmda.split(" ");
        String [] cmdb1 = cmdb.split(" ");
        
        BSPJobClientHelp bspjobclienthelp = new BSPJobClientHelp();
        
        double nResult = bspjobclienthelp.run(cmd1);
        assertEquals("the command is less than 1" ,-1 ,nResult);
        assertEquals("the command is jar" ,0 ,bspjobclienthelp.run(cmd21));
        assertEquals("the command is job" ,0 ,bspjobclienthelp.run(cmd31));
        assertEquals("the command is job -list" ,0 ,bspjobclienthelp.run(cmd41));
        assertEquals("the command is job -kill" ,0 ,bspjobclienthelp.run(cmd51));
        assertEquals("the command is job -list-staffs" ,0 ,bspjobclienthelp.run(cmd61));
        assertEquals("the command is job -setcheckpoiont" ,0 ,bspjobclienthelp.run(cmd71));
        assertEquals("the command is job admin" ,0 ,bspjobclienthelp.run(cmd81));
        assertEquals("the command is job admin -master" ,0 ,bspjobclienthelp.run(cmd91));
        assertEquals("the command is job admin -workers" ,0 ,bspjobclienthelp.run(cmda1));
        assertEquals("the command is job COMMAND LIST" ,0 ,bspjobclienthelp.run(cmdb1));
        
    }

    @Test
    @Ignore("ReadFields(): to be tested")
    public void testMain() {
       
    }

}
