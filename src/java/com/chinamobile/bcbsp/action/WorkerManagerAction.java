/**
 * CopyRight by Chinamobile
 * 
 * WorkerManagerAction.java
 */
package com.chinamobile.bcbsp.action;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * WorkerManager
 * 
 * A generic directive from the {@link com.chinamobile.bcbsp.bsp.BSPController}
 * to the {@link com.chinamobile.bcbsp.bsp.WorkerManager} to take some 'action'.
 * 
 * @author
 * @version
 */
public abstract class WorkerManagerAction implements Writable {

    /**
     * Ennumeration of various 'actions' that the {@link BSPController} directs
     * the {@link WorkerManager} to perform periodically.
     * 
     */
    public static enum ActionType {
        /** Launch a new staff. */
        LAUNCH_STAFF,

        /** Kill a staff. */
        KILL_STAFF,

        /** Kill any tasks of this job and cleanup. */
        KILL_JOB,

        /** Reinitialize the workermanager. */
        REINIT_WORKERMANAGER,

        /** Ask a staff to save its output. */
        COMMIT_STAFF
    };

    /**
     * A factory-method to create objects of given {@link ActionType}.
     * 
     * @param actionType
     *            the {@link ActionType} of object to create.
     * @return an object of {@link ActionType}.
     */
    public static WorkerManagerAction createAction(ActionType actionType) {
        WorkerManagerAction action = null;

        switch (actionType) {
            case LAUNCH_STAFF: {
                action = new LaunchStaffAction();
            }
                break;
            case KILL_STAFF: {
                action = new KillStaffAction();
            }
                break;
            case KILL_JOB: {
                action = new KillJobAction();
            }
                break;
            case REINIT_WORKERMANAGER: {
                action = new ReinitWorkerManagerAction();
            }
                break;
            case COMMIT_STAFF: {
                action = new CommitStaffAction();
            }
                break;
        }

        return action;
    }

    private ActionType actionType;

    protected WorkerManagerAction(ActionType actionType) {
        this.actionType = actionType;
    }

    /**
     * Return the {@link ActionType}.
     * 
     * @return the {@link ActionType}.
     */
    ActionType getActionType() {
        return actionType;
    }

    public void write(DataOutput out) throws IOException {
        WritableUtils.writeEnum(out, actionType);
    }

    public void readFields(DataInput in) throws IOException {
        actionType = WritableUtils.readEnum(in, ActionType.class);
    }
}
