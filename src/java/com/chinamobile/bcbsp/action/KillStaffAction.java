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

import com.chinamobile.bcbsp.util.StaffAttemptID;

/**
 * KillStaffAction
 * 
 * Represents a directive from the
 * {@link com.chinamobile.bcbsp.bsp.BSPController} to the
 * {@link com.chinamobile.bcbsp.bsp.WorkerManager} to kill a staff.
 * 
 * @author
 * @version
 */
public class KillStaffAction extends WorkerManagerAction {
    StaffAttemptID staffId;

    public KillStaffAction() {
        super(ActionType.KILL_STAFF);
        staffId = new StaffAttemptID();
    }

    public KillStaffAction(StaffAttemptID killStaffId) {
        super(ActionType.KILL_STAFF);
        this.staffId = killStaffId;
    }

    public StaffAttemptID getStaffID() {
        return staffId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        staffId.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        staffId.readFields(in);
    }
}
