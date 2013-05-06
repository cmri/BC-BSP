/**
 * StaffSchedular.java
 */
package com.chinamobile.bcbsp.bspcontroller;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import com.chinamobile.bcbsp.workermanager.WorkerManagerControlInterface;

/**
 * Used by a {@link BSPController} to schedule {@link Staff}s on
 * {@link WorkerManager} s.
 * 
 * @author
 * @version
 */
abstract class StaffScheduler implements Configurable {

    protected Configuration conf;
    protected WorkerManagerControlInterface controller;

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public synchronized void setWorkerManagerControlInterface(
            WorkerManagerControlInterface controller) {
        this.controller = controller;
    }

    /**
     * Lifecycle method to allow the scheduler to start any work in separate
     * threads.
     * 
     * @throws IOException
     */
    public void start() throws IOException {
        // do nothing
    }

    /**
     * Lifecycle method to allow the scheduler to stop any work it is doing.
     * 
     * @throws IOException
     */
    public void stop() throws IOException {
        // do nothing
    }

    /**
     * Returns a collection of jobs in an order which is specific to the
     * particular scheduler.
     * 
     * @param Queue
     *            name.
     * @return JobInProgress corresponded to the specified queue.
     */
    public abstract Collection<JobInProgress> getJobs(String queue);
}
