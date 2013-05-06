/**
 * CopyRight by Chinamobile
 * 
 * ClusterUtil.java
 */
package com.chinamobile.bcbsp.util;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.bspcontroller.BSPController;
import com.chinamobile.bcbsp.workermanager.WorkerManager;

/**
 * ClusterUtil
 * 
 * 
 * @author
 * @version
 */
public class ClusterUtil {
    private static final Log LOG = LogFactory.getLog(ClusterUtil.class);

    /**
     * Data Structure to hold WorkerManager Thread and WorkerManager instance
     */
    public static class WorkerManagerThread extends Thread {
        private final WorkerManager workerManager;

        public WorkerManagerThread(final WorkerManager r, final int index) {
            super(r, "WorkerManager:" + index);
            this.workerManager = r;
        }

        /** @return the groom server */
        public WorkerManager getWorkerManager() {
            return this.workerManager;
        }

        /**
         * Block until the groom server has come online, indicating it is ready
         * to be used.
         */
        public void waitForServerOnline() {
            while (!workerManager.isRunning()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // continue waiting
                }
            }
        }
    }

    /**
     * Creates a {@link WorkerManagerThread}. Call 'start' on the returned
     * thread to make it run.
     * 
     * @param c
     *            Configuration to use.
     * @param hrsc
     *            Class to create.
     * @param index
     *            Used distingushing the object returned.
     * @throws IOException
     * @return Groom server added.
     */
    public static ClusterUtil.WorkerManagerThread createWorkerManagerThread(
            final Configuration c, final Class<? extends WorkerManager> hrsc,
            final int index) throws IOException {
        WorkerManager server;
        try {
            server = hrsc.getConstructor(Configuration.class).newInstance(c);
        } catch (Exception e) {
            IOException ioe = new IOException();
            ioe.initCause(e);
            throw ioe;
        }
        return new ClusterUtil.WorkerManagerThread(server, index);
    }

    /**
     * Start the cluster.
     * 
     * @param m
     * @param conf
     * @param groomservers
     * @return Address to use contacting master.
     * @throws InterruptedException
     * @throws IOException
     */
    public static String startup(final BSPController m,
            final List<ClusterUtil.WorkerManagerThread> groomservers,
            Configuration conf) throws IOException, InterruptedException {
        if (m != null) {
            BSPController.startMaster(( BSPConfiguration ) conf);
        }

        if (groomservers != null) {
            for (ClusterUtil.WorkerManagerThread t : groomservers) {
                t.start();
            }
        }

        return m == null ? null : BSPController.getAddress(conf).getHostName();
    }

    public static void shutdown(BSPController master,
            List<WorkerManagerThread> groomThreads, Configuration conf) {
        LOG.debug("Shutting down BC-BSP Cluster");
    }
}
