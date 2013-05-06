/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.comm;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.comm.BSPMessage;

public class BSPMessageTest {

    private  int dstPartition;
    private  String dstVertex;
    private  byte [] tag;
    private  byte [] data; 
    private  static int count = 0;
    @Before
    public void  setUp(){
        
        dstPartition = 1;
        dstVertex = "001";
        tag = Bytes.toBytes("tags");
        data = Bytes.toBytes("data");

    }
    @Test
    public void testBSPMessageIntStringByteArrayByteArray() {
        

        BSPMessage msg = new BSPMessage(dstPartition, dstVertex, tag, data);
        assertEquals("Using dstPartition, dstVertex, tag, data to construct a BSPMessage",
                data.length, msg.getData().length);
    }

    @Test
    public void testBSPMessageStringByteArrayByteArray() {
        BSPMessage msg = new BSPMessage(dstVertex, tag, data);
        assertEquals("Using dstVertex, tag, data to construct a BSPMessage",
                data.length, msg.getData().length);
        assertEquals("Using dstVertex, tag, data to construct a BSPMessage",
                0, msg.getDstPartition());
    }

    @Test
    public void testBSPMessageStringByteArray() {
        BSPMessage msg = new BSPMessage(dstVertex,data);
        assertEquals("Using dstVertex, data to construct a BSPMessage",
                data.length, msg.getData().length);
        assertEquals("Using dstVertex, data to construct a BSPMessage",
                0, msg.getTag().length);
    }

    @Test
    public void testBSPMessageIntStringByteArray() {
        BSPMessage msg = new BSPMessage(dstPartition, dstVertex, data);
        assertEquals("Using dstPartition, dstVertex, data to construct a BSPMessage",
                data.length, msg.getData().length);
        assertEquals("Using dstPartition, dstVertex, data to construct a BSPMessage",
                0, msg.getTag().length);
    }

    @Test
    public void testGetDstPartition() {
        BSPMessage msg = new BSPMessage(dstPartition, dstVertex, tag, data);
        assertEquals("Excepted dstPartitionID is", 1, msg.getDstPartition());
    }

    @Test
    public void testSetDstPartition() {
        BSPMessage msg = new BSPMessage(dstPartition, dstVertex, tag, data);
        msg.setDstPartition(2);
        assertEquals("Excepted dstPartitionID is", 2, msg.getDstPartition());
    }

    @Test
    public void testGetDstVertexID() {
        BSPMessage msg = new BSPMessage(dstPartition, dstVertex, tag, data);
        assertEquals("Excepted dstVertexID is","001", msg.getDstVertexID());
    }

    @Test
    public void testGetTag() {
        BSPMessage msg = new BSPMessage(dstPartition, dstVertex, tag, data);
        
        assertArrayEquals("Excepted tags is",Bytes.toBytes("tags"), msg.getTag());
    }

    @Test
    public void testGetData() {
        BSPMessage msg = new BSPMessage(dstPartition, dstVertex, tag, data);
        assertArrayEquals("Excepted tags is",Bytes.toBytes("data"), msg.getData());
    }

    @Test
    public void testIntoString() {
        BSPMessage msg = new BSPMessage(dstPartition, dstVertex, tag, data);
        String expectedString = this.dstPartition + Constants.SPLIT_FLAG + 
                this.dstVertex + Constants.SPLIT_FLAG + 
                new String(this.tag) + Constants.SPLIT_FLAG + 
                new String(this.data); 
        
        assertEquals("check msg from byte array to string.",expectedString
                , msg.intoString());
    }

    @Test
    public void testFromString() {
        String msgData = this.dstPartition + Constants.SPLIT_FLAG + 
                this.dstVertex + Constants.SPLIT_FLAG + 
                new String(this.tag) + Constants.SPLIT_FLAG + 
                new String(this.data);
        BSPMessage msg = new BSPMessage();
        msg.fromString(msgData);
        
        assertEquals("check msg from string.", 1, msg.getDstPartition());
    }
    @Test
    public void testEmptyMessage(){
        BSPMessage msg = new BSPMessage("001", Bytes.toBytes(""));
        assertEquals("The length of empty message's data is 0 ",
                0, msg.getData().length);
    }
    public static byte[] serialize(Writable writable) throws IOException{
        count++;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        
        writable.write(dataOut);
        dataOut.close();
        
        return out.toByteArray();
    }
    public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException{
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream dataIn = new DataInputStream(in);
        writable.readFields(dataIn);
        dataIn.close();
        return bytes;
    }

}
