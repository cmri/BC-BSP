/**
 * CopyRight by Chinamobile
 * 
 * JobSubmissionProtocol.java
 */
package com.chinamobile.bcbsp.rpc;

import java.io.IOException;

import com.chinamobile.bcbsp.bspcontroller.ClusterStatus;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.JobProfile;
import com.chinamobile.bcbsp.util.JobStatus;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.util.StaffStatus;

/**
 * JobSubmissionProtocol
 * 
 * Protocol that a groom server and the central BSP Master use to communicate.
 * This interface will contains several methods: submitJob, killJob, and
 * killStaff.
 * 
 * @author
 * @version
 */
public interface JobSubmissionProtocol extends BSPRPCProtocolVersion {

    /**
     * Allocate a new id for the job.
     * 
     * @return job id
     * @throws IOException
     */
    public BSPJobID getNewJobId() throws IOException;

    /**
     * Submit a Job for execution. Returns the latest profile for that job. The
     * job files should be submitted in <b>system-dir</b>/<b>jobName</b>.
     * 
     * @param jobID
     * @param jobFile
     * @return jobStatus
     * @throws IOException
     */
    public JobStatus submitJob(BSPJobID jobID, String jobFile)
            throws IOException;

    /**
     * Get the current status of the cluster
     * 
     * @param detailed
     *            if true then report groom names as well
     * @return summary of the state of the cluster
     */
    public ClusterStatus getClusterStatus(boolean detailed) throws IOException;

    /**
     * Grab a handle to a job that is already known to the BSPController.
     * 
     * @return Profile of the job, or null if not found.
     */
    public JobProfile getJobProfile(BSPJobID jobid) throws IOException;

    /**
     * Grab a handle to a job that is already known to the BSPController.
     * 
     * @return Status of the job, or null if not found.
     */
    public JobStatus getJobStatus(BSPJobID jobid) throws IOException;

    /**
     * A BSP system always operates on a single filesystem. This function
     * returns the fs name. ('local' if the localfs; 'addr:port' if dfs). The
     * client can then copy files into the right locations prior to submitting
     * the job.
     */
    public String getFilesystemName() throws IOException;

    /**
     * Get the jobs that are not completed and not failed
     * 
     * @return array of JobStatus for the running/to-be-run jobs.
     */
    public JobStatus[] jobsToComplete() throws IOException;

    /**
     * Get all the jobs submitted.
     * 
     * @return array of JobStatus for the submitted jobs
     */
    
    public JobStatus[] getAllJobs() throws IOException;
    /**
     * get all the staffStatus AttemptID submitted. by yjc
     * @return
     * @throws IOException
     */
    public StaffAttemptID[] getStaffStatus(BSPJobID jobId) throws IOException;
    /**
     * 
     * get staffStatus detail information
     * @return
     * @throws IOException
     */
    public StaffStatus[] getStaffDetail(BSPJobID jobId) throws IOException; 
    /**
     * set checkpoint frquency
     * @param cf
     * @throws IOException
     */
    public void setCheckFrequency(BSPJobID jobID,int cf) throws IOException;
    
    /**
     * Command the job to execute the checkpoint operation at the next superstep.
     * @param jobId
     * @throws IOException
     */
    public void setCheckFrequencyNext(BSPJobID jobId) throws IOException;
    /**
     * Grab the BSPController system directory path where job-specific files are to
     * be placed.
     * 
     * @return the system directory where job-specific files are to be placed.
     */
    public String getSystemDir();

    /**
     * Kill the indicated job
     */
    public void killJob(BSPJobID jobid) throws IOException;

    /**
     * Kill indicated staff attempt.
     * 
     * @param staffId
     *            the id of the staff to kill.
     * @param shouldFail
     *            if true the staff is failed and added to failed staffs list,
     *            otherwise it is just killed, w/o affecting job failure status.
     */
    public boolean killStaff(StaffAttemptID staffId, boolean shouldFail)
            throws IOException;
    public boolean recovery(BSPJobID jobId);
    public void recordFault(Fault f);
        
}
