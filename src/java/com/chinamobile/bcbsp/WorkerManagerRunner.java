/**
 * CopyRight by Chinamobile
 * 
 * WorkerManagerRunner.java
 */
package com.chinamobile.bcbsp;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.chinamobile.bcbsp.util.SystemInfo;
import com.chinamobile.bcbsp.workermanager.WorkerManager;

/**
 * WorkerManagerRunner
 * 
 * This class starts and runs the WorkerManager.
 * It is relative with the shell command.
 * 
 * @author
 * @version
 */
public class WorkerManagerRunner extends Configured implements Tool {

    /**
     * StartupShutdownPretreatment
     * 
     * This class is used to do some prepare work before starting WorkerManager
     * and cleanup after shutting down WorkerManager.
     * For an example, kill all Staff Process before shutting down the damean process.
     * 
     * @author
     * @version
     */
    public class StartupShutdownPretreatment extends Thread {
        
        private final Log LOG;
        private final String hostName;
        private final String className;
        private WorkerManager workerManager;
        
        public StartupShutdownPretreatment( Class<?> clazz, 
                final org.apache.commons.logging.Log LOG) {
            this.LOG = LOG;
            this.hostName = getHostname();
            this.className = clazz.getSimpleName();
            
            this.LOG.info(
                    toStartupShutdownMessage("STARTUP_MSG: ", new String[]{
                            "Starting " + this.className,
                            "  host = " + this.hostName,
                            "  version = " + SystemInfo.getVersionInfo(),
                            "  source = " + SystemInfo.getSourceCodeInfo(),
                            "  compiler = " + SystemInfo.getCompilerInfo()}));
        }
        
        public void setHandler(WorkerManager workerManager) {
            this.workerManager = workerManager;
        }
        
        @Override
        public void run() {
            try { 
                this.workerManager.shutdown();
                
                this.LOG.info(
                        toStartupShutdownMessage("SHUTDOWN_MSG: ", new String[]{
                                "Shutting down " + this.className + " at " + this.hostName}));
            } catch (Exception e) {
                this.LOG.error("Shutdown Abnormally", e);
            }
        }
        
        private String getHostname() {
            try {
                return "" + InetAddress.getLocalHost();
            } catch (UnknownHostException uhe) {
                return "" + uhe;
            }
        }
        
        private String toStartupShutdownMessage(String prefix, String [] msg) {
            StringBuffer b = new StringBuffer(prefix);
            
            b.append("\n/************************************************************");
            for(String s : msg)
              b.append("\n" + prefix + s);
            b.append("\n************************************************************/");
            
            return b.toString();
        }   
    }
    
    public static final Log LOG = LogFactory.getLog(WorkerManagerRunner.class);
    
    @Override
    public int run(String[] args) throws Exception {
        
        StartupShutdownPretreatment pretreatment = new StartupShutdownPretreatment(WorkerManager.class, LOG);
        
        if (args.length != 0) {
            System.out.println("usage: WorkerManagerRunner");
            System.exit(-1);
        }

        try {
            Configuration conf = new BSPConfiguration(getConf());
            WorkerManager workerManager = WorkerManager.constructWorkerManager(
                    WorkerManager.class, conf);

            pretreatment.setHandler(workerManager);
            Runtime.getRuntime().addShutdownHook(pretreatment);
            WorkerManager.startWorkerManager(workerManager).join();
        } catch (Exception e) {
            LOG.fatal("Start Abnormally", e);
            return -1;
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WorkerManagerRunner(), args);
        System.exit(exitCode);
    }
}
