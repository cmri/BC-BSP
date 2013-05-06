/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chinamobile.bcbsp.client;

import java.io.IOException;

import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;

/**
 * RunningJob
 * 
 * It is the user-interface to query for details on a
 * running BSP job.
 * 
 * <p>
 * Clients can get hold of <code>RunningJob</code> via the {@link BSPJobClient}
 * and then query the running-job for details such as name, configuration,
 * progress etc.
 * </p>
 * 
 * @author
 * @version
 * @see BSPJobClient
 */
public interface RunningJob {
    /**
     * Get the job identifier.
     * 
     * @return the job identifier.
     */
    public BSPJobID getID();

    /**
     * Get the name of the job.
     * 
     * @return the name of the job.
     */
    public String getJobName();

    /**
     * Get the path of the submitted job configuration.
     * 
     * @return the path of the submitted job configuration.
     */
    public String getJobFile();

    /**
     * Get the <i>progress</i> of the job's staffs, as a float between 0.0 and
     * 1.0. When all bsp staffs have completed, the function returns 1.0.
     * 
     * @return the progress of the job's staffs.
     * @throws IOException
     */
    public long progress() throws IOException;

    /**
     * Check if the job is finished or not. This is a non-blocking call.
     * 
     * @return <code>true</code> if the job is complete, else <code>false</code>
     *         .
     * @throws IOException
     */
    public boolean isComplete() throws IOException;

    /**
     * Check if the job completed successfully.
     * 
     * @return <code>true</code> if the job succeeded, else <code>false</code>.
     * @throws IOException
     */
    public boolean isSuccessful() throws IOException;
    
    /**
     * Check if the job is killed by user.
     * 
     * @return <code>true</code> if the job is killed, else <code>false</code>.
     * @throws IOException
     */
    public boolean isKilled() throws IOException;
    
    /**
     * Check if the job is failed.
     * 
     * @return <code>true</code> if the job is failed, else <code>false</code>.
     * @throws IOException
     */
    public boolean isFailed() throws IOException;
    
    /**
     * Check if the job is recovery.
     * 
     * @return <code>true</code> if the is recovery, else <code>false</code>.
     * @throws IOException
     */
    public boolean isRecovery() throws IOException;

    /**
     * Blocks until the job is complete.
     * 
     * @throws IOException
     */
    public void waitForCompletion() throws IOException;

    /**
     * Returns the current state of the Job. {@link JobStatus}
     * 
     * @throws IOException
     */
    public int getJobState() throws IOException;

    /**
     * Kill the running job. Blocks until all job staffs have been killed as
     * well. If the job is no longer running, it simply returns.
     * 
     * @throws IOException
     */
    public void killJob() throws IOException;

    /**
     * Kill indicated staff attempt.
     * 
     * @param staffId
     *            the id of the staff to be terminated.
     * @param shouldFail
     *            if true the staff is failed and added to failed staffs list,
     *            otherwise it is just killed, w/o affecting job failure status.
     * @throws IOException
     */
    public void killStaff(StaffAttemptID staffId, boolean shouldFail)
            throws IOException;

    public long getSuperstepCount() throws IOException;
}
