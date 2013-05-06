/**
 * CopyRight by Chinamobile
 * 
 * BSPControllerRunner.java
 */
package com.chinamobile.bcbsp;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.chinamobile.bcbsp.bspcontroller.BSPController;
import com.chinamobile.bcbsp.util.SystemInfo;

/**
 * BSPControllerRunner
 * 
 * This class starts and runs the BSPController.
 * It is relative with the shell command.
 * 
 * @author
 * @version
 */
public class BSPControllerRunner extends Configured implements Tool {

    /**
     * StartupShutdownPretreatment
     * 
     * This class is used to do some prepare work before starting BSPController
     * and cleanup after shutting down BSPController.
     * 
     * @author
     * @version
     */
    public class StartupShutdownPretreatment extends Thread {
        
        private final Log LOG;
        private final String hostName;
        private final String className;
        private BSPController bspController;
        
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
        
        public void setHandler(BSPController bspController) {
            this.bspController = bspController;
        }
        
        @Override
        public void run() {
            try {
                this.bspController.shutdown();
                
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
    
    public static final Log LOG = LogFactory.getLog(BSPControllerRunner.class);

    @Override
    public int run(String[] args) throws Exception {
        StartupShutdownPretreatment pretreatment = 
                new StartupShutdownPretreatment(BSPController.class, LOG);

        if (args.length != 0) {
            System.out.println("usage: BSPControllerRunner");
            System.exit(-1);
        }

        try {
            BSPConfiguration conf = new BSPConfiguration(getConf());
            BSPController bspController = BSPController.startMaster(conf);
            pretreatment.setHandler(bspController);
            Runtime.getRuntime().addShutdownHook(pretreatment);
            bspController.offerService();
        } catch (Exception e) {
            LOG.fatal("Start Abnormally", e);
            return -1;
        }
        
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BSPControllerRunner(), args);
        System.exit(exitCode);
    }
}
