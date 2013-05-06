/**
 * CopyRight by Chinamobile
 */

package com.chinamobile.bcbsp.io;

import java.io.IOException;

import com.chinamobile.bcbsp.util.BSPJob;

/**
 * RecordWriter
 * 
 * This class can write a record in the format of Key-Value.
 * 
 * @author
 * @version
 */
public abstract class RecordWriter<K, V> {

    /**
     * Writes a key/value pair. <code>RecordWriter</code> writes the output
     * &lt;key, value&gt; pairs to an output file.
     * 
     * @param key
     *            the key to write.
     * @param value
     *            the value to write.
     * @throws IOException
     */
    public abstract void write(K key, V value) throws IOException,
            InterruptedException;

    /**
     * Close this <code>RecordWriter</code> to future operations.
     * 
     * @param context
     *            the context of the task
     * @throws IOException
     */
    public abstract void close(BSPJob job) throws IOException,
            InterruptedException;
}
