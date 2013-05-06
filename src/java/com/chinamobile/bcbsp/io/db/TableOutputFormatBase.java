/**
 * CopyRight by Chinamobile
 * 
 * TableOutputFormatBase.java
 */
package com.chinamobile.bcbsp.io.db;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import com.chinamobile.bcbsp.io.OutputFormat;
import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;

/**
 * Convert BCBSP Job output and write it to an HBase table.
 */
public class TableOutputFormatBase<Text> extends OutputFormat<Text, Text> {

    private HTable table;

    /**
     * Creates a new record writer.
     * 
     * @param job
     *            The current job BSPJob.
     * @return The newly created writer instance.
     * @throws IOException
     *             When creating the writer fails.
     * @throws InterruptedException
     *             When the jobs is can celled. 
     */
    @Override
    public RecordWriter<Text, Text> getRecordWriter(BSPJob job,
            StaffAttemptID staffId) throws IOException, InterruptedException {
        return new TableRecordWriter<Text>(this.table);
    }

    /**
     * Set the {@link Path} of the output directory for the BC-BSP job.
     * 
     * @param job
     *            The job configuration
     * @param outputDir
     *            the {@link Path} of the output directory for the BC-BSP job.
     * @throws InterruptedException
     * @throws IOException
     */
    public static void setOutputPath(BSPJob job, Path outputDir) {

    }

    /**
     * Get the {@link Path} to the output directory for the BC-BSP job.
     * 
     * @return the {@link Path} to the output directory for the BC-BSP job.
     */
    public static Path getOutputPath(BSPJob job, StaffAttemptID staffId) {
        return null;
    }

    /**
     * Checks if the output target exists.
     * 
     * @param context
     *            The current context.
     * @throws IOException
     *             When the check fails.
     * @throws InterruptedException
     *             When the job is aborted.
     * @see org.apache.hadoop.mapreduce.OutputFormat#checkOutputSpecs(org.apache.hadoop.mapreduce.JobContext)
     */
    public static void checkOutputSpecs(BSPJob job, Path path)
            throws IOException, InterruptedException {
        // TODO Check if the table exists?

    }

    public HTable getTable() {
        return table;
    }

    public void setTable(HTable table) {
        this.table = table;
    }

    @Override
    public RecordWriter<org.apache.hadoop.io.Text, org.apache.hadoop.io.Text> getRecordWriter(
            BSPJob job, StaffAttemptID staffId, Path writePath)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }
}
