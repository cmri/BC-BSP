/**
 * CopyRight by Chinamobile
 * 
 * Schedulable.java
 */
package com.chinamobile.bcbsp.bspcontroller;

import java.io.IOException;

/**
 * This is the class that schedules commands to WorkerManager(s)
 * 
 * @author
 * @version
 */
public interface Schedulable {

    /**
     * Schedule job to designated WorkerManager(s) immediately.
     * 
     * @param jip
     *            to be scheduled.
     * @param statuses
     *            of WorkerManager(s).
     * @throws IOException
     */
    void normalSchedule(JobInProgress jip) throws IOException;
    void recoverySchedule(JobInProgress jip) throws IOException;
}
