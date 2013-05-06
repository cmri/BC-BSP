/**
 * StaffRunner.java
 */
package com.chinamobile.bcbsp.bspstaff;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.RunJar;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.workermanager.WorkerManager;

/**
 * StaffRunner
 * 
 * Base class that runs a staff in a child process.
 * 
 * @author
 * @version
 */

public class StaffRunner extends Thread {

    public static final Log LOG = LogFactory.getLog(StaffRunner.class);
    boolean killed = false;
    private Process process;
    private Staff staff;
    private BSPJob conf;
    private WorkerManager workerManager;
    private int faultSSStep = 0;

    public StaffRunner(BSPStaff bspStaff, WorkerManager workerManager,
            BSPJob conf) {
        this.staff = bspStaff;
        this.conf = conf;
        this.workerManager = workerManager;
    }

    public Staff getStaff() {
        return staff;
    }
    
    public int getFaultSSStep() {
        return faultSSStep;
    }

    public void setFaultSSStep(int faultSSStep) {
        this.faultSSStep = faultSSStep;
    }

    /**
     * Called to assemble this staff's input. This method is run in the parent
     * process before the child is spawned. It should not execute user code,
     * only system code.
     * 
     * @return
     * @throws IOException
     */
    public boolean prepare() throws IOException {
        return true;
    }

    public void run() {

        try {
            String sep = System.getProperty("path.separator");
            File workDir = new File(new File(staff.getJobFile()).getParent(),
                    "work");
            boolean isCreated = workDir.mkdirs();
            if (!isCreated) {
                LOG.debug("StaffRunner.workDir : " + workDir);
            }

            StringBuffer classPath = new StringBuffer();
            classPath.append(System.getProperty("java.class.path"));
            classPath.append(sep);
            String jar = conf.getJar();
            // if jar exists, it into workDir
            if (jar != null) {
                RunJar.unJar(new File(jar), workDir);
                File[] libs = new File(workDir, "lib").listFiles();
                if (libs != null) {
                    for (int i = 0; i < libs.length; i++) {
                        // add libs from jar to classpath
                        classPath.append(sep);
                        classPath.append(libs[i]);
                    }
                }
                classPath.append(sep);
                classPath.append(new File(workDir, "classes"));
                classPath.append(sep);
                classPath.append(workDir);
            }

            // Build exec child jmv args.
            Vector<String> vargs = new Vector<String>();
            File jvm = new File(
                    new File(System.getProperty("java.home"), "bin"), "java");
            vargs.add(jvm.toString());

            // bsp.child.java.opts
            String javaOpts = conf.getConf().get("bsp.child.java.opts",
                    "-Xmx200m");
            javaOpts = javaOpts.replace("@taskid@", staff.getStaffID()
                    .toString());
            String[] javaOptsSplit = javaOpts.split(" ");
            for (int i = 0; i < javaOptsSplit.length; i++) {
                vargs.add(javaOptsSplit[i]);
            }

            // Add classpath.
            vargs.add("-classpath");
            vargs.add(classPath.toString());

            // Setup the log4j prop
            long logSize = StaffLog
                    .getStaffLogLength((( BSPConfiguration ) conf.getConf()));
            vargs.add("-Dbcbsp.log.dir="
                    + new File(System.getProperty("bcbsp.log.dir"))
                            .getAbsolutePath());
            vargs.add("-Dbcbsp.root.logger=INFO,TLA");
            vargs.add("-Dbcbsp.tasklog.taskid=" + staff.getStaffID());
            vargs.add("-Dbcbsp.tasklog.totalLogFileSize=" + logSize);

            // Add main class and its arguments
            vargs.add(WorkerManager.Child.class.getName());
            InetSocketAddress addr = workerManager
                    .getStaffTrackerReportAddress();
            
            vargs.add(addr.getHostName());
            vargs.add(Integer.toString(addr.getPort()));
            vargs.add(staff.getStaffID().toString());
            vargs.add(Integer.toString(getFaultSSStep()));
            vargs.add(workerManager.getHostName());

            // Run java
            runChild(( String[] ) vargs.toArray(new String[0]), workDir);
        } catch (Exception e) {
            LOG.error("[run]", e);
        }
    }

    /**
     * Run the child process
     */
    private void runChild(String[] args, File dir) throws Exception {
        this.process = Runtime.getRuntime().exec(args, null, dir);
        try {
            int exit_code = process.waitFor();
            if (!killed && exit_code != 0) {
                throw new Exception(
                        "Staff process exit with nonzero status of "
                                + exit_code + ".");
            }

        } catch (InterruptedException e) {
            throw new IOException(e.toString());
        } finally {
            kill();
        }
    }

    /**
     * Kill the child process
     */
    public void kill() {
        if (process != null) {
            process.destroy();
        }
        killed = true;
    }
}
