/**
 * CopyRight by Chinamobile
 * 
 * BSPFileOutputFormat.java
 */

package com.chinamobile.bcbsp.io;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.Constants;

/**
 * BSPFileOutputFormat
 * 
 * This class is used for writing on the file system, such as HDFS.
 * 
 * @author
 * @version
 */
public abstract class BSPFileOutputFormat<K, V> extends OutputFormat<K, V> {

    private static final Log LOG = LogFactory.getLog(BSPFileOutputFormat.class);

    /**
     * If the output directory has existed, then delete it.
     */
    public static void checkOutputSpecs(BSPJob job, Path outputDir) {
        try {
            FileSystem fileSys = FileSystem.get(job.getConf());
            if (fileSys.exists(outputDir)) {
                fileSys.delete(outputDir, true);
            }
        } catch (IOException e) {
            LOG.error("checkOutputSpecs", e);
        }

    }

    /**
     * Set the {@link Path} of the output directory for the BC-BSP job.
     * 
     * @param job
     *            The job configuration
     * @param outputDir
     *            the {@link Path} of the output directory for the BC-BSP job.
     */
    public static void setOutputPath(BSPJob job, Path outputDir) {
        Configuration conf = job.getConf();
        checkOutputSpecs(job, outputDir);
        conf.set(Constants.USER_BC_BSP_JOB_OUTPUT_DIR, outputDir.toString());
    }

    /**
     * Get the {@link Path} to the output directory for the BC-BSP job.
     * 
     * @return the {@link Path} to the output directory for the BC-BSP job.
     */
    public static Path getOutputPath(BSPJob job, StaffAttemptID staffId) {
        String name = job.getConf().get(Constants.USER_BC_BSP_JOB_OUTPUT_DIR)
                + "/" + "staff-" + staffId.toString().substring(26, 32);
        return name == null ? null : new Path(name);
    }
    
    public static Path getOutputPath(StaffAttemptID staffId, Path writePath) {
        String name = writePath.toString() + "/" + staffId.toString()
        + "/checkpoint.cp";
        return name == null ? null : new Path(name);
    }
}






