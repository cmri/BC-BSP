/**
 * CopyRight by Chinamobile
 * 
 * TextBSPFileOutputFormat.java
 */

package com.chinamobile.bcbsp.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.Text;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;

/**
 * TextBSPFileOutputFormat
 * 
 * An example that extends BSPFileOutputFormat<Text,Text>.
 * 
 * @author
 * @version 
 */
public class TextBSPFileOutputFormat extends BSPFileOutputFormat<Text, Text> {

    public static class LineRecordWriter extends RecordWriter<Text, Text> {
        private static final String utf8 = "UTF-8";
        private static final byte[] newline;
        static {
            try {
                newline = "\n".getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8
                        + " encoding");
            }
        }

        protected DataOutputStream out;
        private final byte[] keyValueSeparator;

        public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
            this.out = out;
            try {
                this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8
                        + " encoding");
            }
        }

        public LineRecordWriter(DataOutputStream out) {
            this(out, Constants.KV_SPLIT_FLAG);
        }

        /**
         * Write the object to the byte stream, handling Text as a special case.
         * 
         * @param o
         *            the object to print
         * @throws IOException
         *             if the write throws, we pass it on
         */
        private void writeObject(Object o) throws IOException {
            if (o instanceof Text) {
                Text to = ( Text ) o;
                out.write(to.getBytes(), 0, to.getLength());
            } else {
                out.write(o.toString().getBytes(utf8));
            }
        }

        @Override
        public void write(Text key, Text value) throws IOException {
            boolean nullKey = (key == null);
            boolean nullValue = (value == null);
            if (nullKey && nullValue) {
                return;
            }
            if (!nullKey) {
                writeObject(key);
            }
            if (!(nullKey || nullValue)) {
                out.write(keyValueSeparator);
            }
            if (!nullValue) {
                writeObject(value);
            }
            out.write(newline);
        }

        @Override
        public void close(BSPJob job) throws IOException {
            out.close();
        }
    }

    @Override
    public RecordWriter<Text, Text> getRecordWriter(BSPJob job,
            StaffAttemptID staffId) throws IOException, InterruptedException {
        Path file = getOutputPath(job, staffId);
        FileSystem fs = file.getFileSystem(job.getConf());
        FSDataOutputStream fileOut = fs.create(file, false);
        return new LineRecordWriter(fileOut);
    }

    @Override
    public RecordWriter<Text, Text> getRecordWriter(BSPJob job,
            StaffAttemptID staffId, Path writePath) throws IOException, InterruptedException {
        Path file = getOutputPath(staffId, writePath);
        FileSystem fs = file.getFileSystem(job.getConf());
        FSDataOutputStream fileOut = fs.create(file, false);
        return new LineRecordWriter(fileOut);
    }
}









