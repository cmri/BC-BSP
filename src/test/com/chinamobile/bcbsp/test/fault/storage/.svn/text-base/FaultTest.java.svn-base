/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.fault.storage;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;
import com.chinamobile.bcbsp.fault.storage.Fault;

public class FaultTest {

    @Test
    public void testWriteAndReadFields() throws IOException {
        int superstep = 1;
        Fault fault = new Fault(Fault.Type.DISK, Fault.Level.CRITICAL, "workerNodeName",
                "exceptionMessage", "jobName", "staffName",superstep );
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        
        fault.write(dataOut);
        dataOut.close();
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        DataInputStream dataIn = new DataInputStream(in);
        Fault faultin = new Fault();
        faultin.readFields(dataIn);
        dataIn.close();
        assertEquals(superstep,faultin.getSuperStep_Stage());
        
    }



    @Test
    public void testToString() {
        int superstep = 1;
        Fault fault = new Fault(Fault.Type.DISK, Fault.Level.CRITICAL, "workerNodeName",
                "exceptionMessage", "jobName", "staffName",superstep );
        String strFormat = fault.toString();
        String expectStr = "2012/10/23 09:42:59,124--DISK--CRITICAL--workerNodeName--jobName--staffName--exceptionMessage--true--1"; 
        assertEquals(expectStr.subSequence(23, expectStr.length()-1), strFormat.subSequence(23, strFormat.length()-1));
    }

    @Test
    public void testEqualsObject() {
        int superstep = 1;
        Fault fault1 = new Fault(Fault.Type.DISK, Fault.Level.CRITICAL, "workerNodeName",
                "exceptionMessage", "jobName", "staffName",superstep );
        Fault fault2 = new Fault(Fault.Type.DISK, Fault.Level.CRITICAL, "workerNodeName",
                "exceptionMessage", "jobName", "staffName",superstep );
        SimpleDateFormat simp = new SimpleDateFormat();
        String  timeOfFailure = simp.format(new Date());
        fault1.setTimeOfFailure(timeOfFailure);
        fault2.setTimeOfFailure(timeOfFailure);
        assertEquals(true,fault1.equals(fault2));
    }


    

}
