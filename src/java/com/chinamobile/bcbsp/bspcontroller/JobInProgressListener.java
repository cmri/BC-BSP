/**
 * JobInProgressListener.java
 */
package com.chinamobile.bcbsp.bspcontroller;

import java.io.IOException;
import java.util.ArrayList;

import com.chinamobile.bcbsp.util.BSPJobID;

/**
 * A listener for changes in a {@link JobInProgress job}'s lifecycle in the
 * {@link BSPController}.
 * 
 * @author
 * @version
 */
public abstract class JobInProgressListener {

    /**
     * Invoked when a new job has been added to the {@link BSPController}.
     * 
     * @param jip
     *            The job to be added.
     * @throws IOException
     */
    public abstract void jobAdded(JobInProgress jip) throws IOException;

    /**
     * Invoked when a job has been removed from the {@link BSPController}.
     * 
     * @param jip
     *            The job to be removed .
     * @throws IOException
     */
    public abstract ArrayList<BSPJobID> jobRemoved(JobInProgress jip) throws IOException;
    
}
