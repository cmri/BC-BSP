/**
 * CopyRight by Chinamobile
 * 
 * TableRecordWriter.java
 */
package com.chinamobile.bcbsp.io.db;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.util.BSPJob;

/**
 * Writes the output to an HBase table.
 * 
 * @param <Text>
 *            The type of the key.
 */
public class TableRecordWriter<Text> extends RecordWriter<Text,Text> {

    /** The table to write to. */
    private HTable table;
    
    /**
     * Instantiate a TableRecordWriter with the HBase HClient for writing.
     * 
     * @param table
     *            The table to write to.
     */
    public TableRecordWriter(HTable table) {
        this.table = table;
    }

   /**
    * Closes the writer, in this case flush table commits.
    */
    @Override
    public void close(BSPJob job) throws IOException {
        table.flushCommits();
        // The following call will shutdown all connections to the cluster
        // from
        // this JVM. It will close out our zk session otherwise zk wil log
        // expired sessions rather than closed ones. If any other HTable
        // instance
        // running in this JVM, this next call will cause it damage.
        // Presumption
        // is that the above this.table is only instance.
        HConnectionManager.deleteAllConnections(true);
    }

    /**
     * Writes a key/value pair into the table.
     * 
     * @param key
     *            The key.
     * @param value
     *            The value.
     * @throws IOException
     *             When writing fails.
     * @see com.chinamobile.bcbsp.io.RecordWriter#write(java.lang.Object,
     *      java.lang.Object)
     */
    @Override
    public void write(Text key, Text value) throws IOException {
        Put put=new Put(key.toString().getBytes());
        put.add("BorderNode".getBytes(),"nodeData".getBytes(), value.toString().getBytes());
        this.table.put(put);
    }
}
