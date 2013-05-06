/**
 * CopyRight by Chinamobile
 * 
 * TableRecordReader.java
 */
package com.chinamobile.bcbsp.io.db;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;

import com.chinamobile.bcbsp.io.RecordReader;

/**
 * Iterate over an HBase table data, return (ImmutableBytesWritable, Result)
 * pairs.
 */
public class TableRecordReader extends RecordReader<Text, Text> {

    private TableRecordReaderImpl recordReaderImpl = new TableRecordReaderImpl();
    Text key = new Text();
    Text value = new Text();

    /**
     * Restart from survivable exceptions by creating a new scanner.
     * 
     * @param firstRow
     *            The first row to start at.
     * @throws IOException
     *             When restarting fails.
     */
    public void restart(byte[] firstRow) throws IOException {
        this.recordReaderImpl.restart(firstRow);
    }

    /**
     * Build the scanner. Not done in constructor to allow for extension.
     * 
     * @throws IOException
     *             When restarting the scan fails.
     */
    public void init() throws IOException {
        this.recordReaderImpl.init();
    }

    /**
     * Sets the HBase table.
     * 
     * @param htable
     *            The {@link HTable} to scan.
     */
    public void setHTable(HTable htable) {
        this.recordReaderImpl.setHTable(htable);
    }

    /**
     * Sets the scan defining the actual details like columns etc.
     * 
     * @param scan
     *            The scan to set.
     */
    public void setScan(Scan scan) {
        this.recordReaderImpl.setScan(scan);
    }

    /**
     * Closes the split.
     * 
     * @see org.apache.hadoop.mapreduce.RecordReader#close()
     */
    @Override
    public void close() {
        this.recordReaderImpl.close();
    }

    /**
     * Returns the current key.
     * 
     * @return The current key.
     * @throws IOException
     * @throws InterruptedException
     *             When the job is aborted.
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        key.set(new String(this.recordReaderImpl.getCurrentKey().get()));
        return key;
    }

    /**
     * Returns the current value.
     * 
     * @return The current value.
     * @throws IOException
     *             When the value is faulty.
     * @throws InterruptedException
     *             When the job is aborted.
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
     */
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        value.set(new String(this.recordReaderImpl.getCurrentValue()
                .getValue("BorderNode".getBytes(), "nodeData".getBytes())
            ));
        return value;
    }

    /**
     * Initializes the reader.
     * 
     * @param inputsplit
     *            The split to work with.
     * @param context
     *            The current task context.
     * @throws IOException
     *             When setting up the reader fails.
     * @throws InterruptedException
     *             When the job is aborted.
     */
    @Override
    public void initialize(InputSplit inputsplit, Configuration conf)
            throws IOException, InterruptedException {
    }

    /**
     * Positions the record reader to the next record.
     * 
     * @return <code>true</code> if there was another record.
     * @throws IOException
     *             When reading the record failed.
     * @throws InterruptedException
     *             When the job was aborted.
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return this.recordReaderImpl.nextKeyValue();
    }

    /**
     * The current progress of the record reader through its data.
     * 
     * @return A number between 0.0 and 1.0, the fraction of the data read.
     */
    @Override
    public float getProgress() {
        return this.recordReaderImpl.getProgress();
    }


}
