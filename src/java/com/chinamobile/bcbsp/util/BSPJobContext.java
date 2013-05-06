/**
 * BSPJobContext.java
 */
package com.chinamobile.bcbsp.util;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;

/**
 * BSPJobContext
 * 
 * This class is a base job configuration information container.
 * 
 * @author
 * @version
 */
public class BSPJobContext {
    protected final Configuration conf;
    private final BSPJobID jobId;

    public BSPJobContext(Configuration conf, BSPJobID jobId) {
        this.conf = conf;
        this.jobId = jobId;
    }

    public BSPJobContext(Path config, BSPJobID jobId) throws IOException {
        this.conf = new BSPConfiguration();
        this.jobId = jobId;
        this.conf.addResource(config);
    }

    public BSPJobID getJobID() {
        return jobId;
    }

    /**
     * Constructs a local file name. Files are distributed among configured
     * local directories.
     * 
     * @param pathString
     * @return
     * @throws IOException
     */
    public Path getLocalPath(String pathString) throws IOException {
        return conf.getLocalPath(Constants.BC_BSP_LOCAL_DIRECTORY, pathString);
    }

    public void writeXml(OutputStream out) throws IOException {
        conf.writeXml(out);
    }

    public Configuration getConf() {
        return this.conf;
    }

    public void set(String name, String value) {
        conf.set(name, value);
    }

    public String get(String name) {
        return conf.get(name);
    }
    
    public String get(String name, String defaultValue) {
        return conf.get(name, defaultValue);
    }

    public void setInt(String name, int value) {
        conf.setInt(name, value);
    }

    public int getInt(String name, int defaultValue) {
        return conf.getInt(name, defaultValue);
    }
}
