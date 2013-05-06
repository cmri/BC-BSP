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
package com.chinamobile.bcbsp.action;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.bspstaff.BSPStaff;

/**
 * LaunchStaffAction
 * 
 * Represents a directive from the
 * {@link com.chinamobile.bcbsp.bsp.BSPController} to the
 * {@link com.chinamobile.bcbsp.bsp.WorkerManager} to launch a new staff.
 * 
 * @author
 * @version
 */
public class LaunchStaffAction extends WorkerManagerAction {

    public static final Log LOG = LogFactory.getLog(LaunchStaffAction.class);
    private Staff staff;

    public LaunchStaffAction() {
        super(ActionType.LAUNCH_STAFF);
    }

    public LaunchStaffAction(Staff staff) {
        super(ActionType.LAUNCH_STAFF);
        this.staff = staff;
    }
    
    public Staff getStaff() {
        return this.staff;
    }

    public void write(DataOutput out) throws IOException {
        staff.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        staff = new BSPStaff();
        staff.readFields(in);
    }
}
