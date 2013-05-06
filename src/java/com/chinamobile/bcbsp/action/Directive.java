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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.chinamobile.bcbsp.workermanager.WorkerManagerStatus;

/**
 * Directive
 * 
 * A generic directive from the {@link com.chinamobile.bcbsp.bsp.BSPController}
 * to the {@link com.chinamobile.bcbsp.bsp.WorkerManager} to take some 'action'.
 * 
 * @author
 * @version
 */

public class Directive implements Writable {

    public static final Log LOG = LogFactory.getLog(Directive.class);

    private long timestamp;
    private Directive.Type type;
    private String[] workerManagersName;
    private WorkerManagerAction[] actions;
    private WorkerManagerStatus status;
    private int faultSSStep;

    
    public static enum Type {
        Request(1), Response(2);
        int t;

        Type(int t) {
            this.t = t;
        }

        public int value() {
            return this.t;
        }
    };

    public Directive() {
        this.timestamp = System.currentTimeMillis();
        
    }

    public Directive(String[] workerManagersName, WorkerManagerAction[] actions) {
        this();
        this.type = Directive.Type.Request;
        this.workerManagersName = workerManagersName;
        this.actions = actions;
    }

    public Directive(WorkerManagerStatus status) {
        this();
        this.type = Directive.Type.Response;
        this.status = status;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public Directive.Type getType() {
        return this.type;
    }

    public String[] getWorkerManagersName() {
        return this.workerManagersName;
    }

    public WorkerManagerAction[] getActions() {
        return this.actions;
    }

    public WorkerManagerStatus getStatus() {
        return this.status;
    }

    public int getFaultSSStep() {
        return faultSSStep;
    }

    public void setFaultSSStep(int faultSSStep) {
        this.faultSSStep = faultSSStep;
    }
    
    public void write(DataOutput out) throws IOException {
        out.writeInt(faultSSStep);
        out.writeLong(this.timestamp);
        out.writeInt(this.type.value());
        if (getType().value() == Directive.Type.Request.value()) {
            if (this.actions == null) {
                WritableUtils.writeVInt(out, 0);
            } else {
                WritableUtils.writeVInt(out, actions.length);
                for (WorkerManagerAction action : this.actions) {
                    WritableUtils.writeEnum(out, action.getActionType());
                    action.write(out);
                }
            }

            WritableUtils.writeCompressedStringArray(out,
                    this.workerManagersName);
        } else if (getType().value() == Directive.Type.Response.value()) {
            this.status.write(out);
        } else {
            throw new IllegalStateException("Wrong directive type:" + getType());
        }

    }

    public void readFields(DataInput in) throws IOException {
        this.faultSSStep = in.readInt();
        this.timestamp = in.readLong();
        int t = in.readInt();
        if (Directive.Type.Request.value() == t) {
            this.type = Directive.Type.Request;
            int length = WritableUtils.readVInt(in);
            if (length > 0) {
                this.actions = new WorkerManagerAction[length];
                for (int i = 0; i < length; ++i) {
                    WorkerManagerAction.ActionType actionType = WritableUtils
                            .readEnum(in, WorkerManagerAction.ActionType.class);
                    actions[i] = WorkerManagerAction.createAction(actionType);
                    actions[i].readFields(in);
                }
            } else {
                this.actions = null;
            }

            this.workerManagersName = WritableUtils
                    .readCompressedStringArray(in);
        } else if (Directive.Type.Response.value() == t) {
            this.type = Directive.Type.Response;
            this.status = new WorkerManagerStatus();
            this.status.readFields(in);
        } else {
            throw new IllegalStateException("Wrong directive type:" + t);
        }
    }
}
