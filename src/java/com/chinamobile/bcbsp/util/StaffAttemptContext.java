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
package com.chinamobile.bcbsp.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;

/**
 * StaffAttempContext
 * 
 * The context for Staff attempts.
 * 
 * @author
 * @version
 */
public class StaffAttemptContext extends BSPJobContext implements Progressable {
    private final StaffAttemptID staffId;
    private String status = "";

    public StaffAttemptContext(Configuration conf, StaffAttemptID staffId) {
        super(conf, staffId.getJobID());
        this.staffId = staffId;
    }

    /**
     * Get the unique name for this staff attempt.
     */
    public StaffAttemptID getStaffAttemptID() {
        return staffId;
    }

    /**
     * Set the current status of the staff to the given string.
     */
    public void setStatus(String msg) throws IOException {
        status = msg;
    }

    /**
     * Get the last set status message.
     * 
     * @return the current status message
     */
    public String getStatus() {
        return status;
    }

    /**
     * Report progress. The subtypes actually do work in this method.
     */
    public void progress() {
    }
}
