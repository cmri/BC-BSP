/**
 * CopyRight by Chinamobile
 * 
 * OutPutFormat.java
 */

package com.chinamobile.bcbsp.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;

/**
 * OutputFormat
 * 
 * This is an abstract class. All user-defined OutputFormat class must implement
 * the methods:getRecordWriter();
 * 
 * @author
 * @version
 */
public abstract class OutputFormat<K, V> {

    /**
     * Get the {@link RecordWriter} for the given staff.
     * 
     * @param job
     *            the information about the current staff.
     * @return a {@link RecordWriter} to write the output for the job.
     * @throws IOException
     */
    public abstract RecordWriter<K, V> getRecordWriter(BSPJob job,
            StaffAttemptID staffId) throws IOException, InterruptedException;
    
    public abstract RecordWriter<Text, Text> getRecordWriter(BSPJob job,
            StaffAttemptID staffId, Path writePath) throws IOException, InterruptedException;

    /**
     * This method is only used to write data into HBase. If the data is wrote
     * into the DFS you do not cover it. This method is primarily used to
     * initialize the HBase table.
     * 
     * @param configuration
     */
    public void initialize(Configuration otherConf) {

    }
}
