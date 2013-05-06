/**
 * CopyRight by Chinamobile
 * 
 * CommitStaffAction.java
 */
package com.chinamobile.bcbsp.action;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.chinamobile.bcbsp.util.StaffAttemptID;

/**
 * CommitStaffAction
 * 
 * 
 * @author
 * @version
 */
class CommitStaffAction extends WorkerManagerAction {
    private StaffAttemptID staffId;

    public CommitStaffAction() {
        super(ActionType.COMMIT_STAFF);
        staffId = new StaffAttemptID();
    }

    public CommitStaffAction(StaffAttemptID staffId) {
        super(ActionType.COMMIT_STAFF);
        this.staffId = staffId;
    }

    public StaffAttemptID getStaffID() {
        return staffId;
    }

    public void write(DataOutput out) throws IOException {
        staffId.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        staffId.readFields(in);
    }
}
