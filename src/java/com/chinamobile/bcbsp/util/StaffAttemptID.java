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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * StaffAttemptID
 * 
 * StaffAttemptID is a unique identifier for a staff attempt.
 */
public class StaffAttemptID extends ID {
    protected static final String ATTEMPT = "attempt";
    private StaffID staffId;

    public StaffAttemptID(StaffID staffId, int id) {
        super(id);
        if (staffId == null) {
            throw new IllegalArgumentException("staffId cannot be null");
        }
        this.staffId = staffId;
    }

    public StaffAttemptID(String jtIdentifier, int jobId, int staffId, int id) {
        this(new StaffID(jtIdentifier, jobId, staffId), id);
    }

    public StaffAttemptID() {
        staffId = new StaffID();
    }

    public BSPJobID getJobID() {
        return staffId.getJobID();
    }

    public StaffID getStaffID() {
        return staffId;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o))
            return false;

        StaffAttemptID that = ( StaffAttemptID ) o;
        return this.staffId.equals(that.staffId);
    }

    protected StringBuilder appendTo(StringBuilder builder) {
        return staffId.appendTo(builder).append(SEPARATOR).append(id);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        staffId.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        staffId.write(out);
    }

    @Override
    public int hashCode() {
        return staffId.hashCode() * 5 + id;
    }

    @Override
    public int compareTo(ID o) {
        StaffAttemptID that = ( StaffAttemptID ) o;
        int tipComp = this.staffId.compareTo(that.staffId);
        if (tipComp == 0) {
            return this.id - that.id;
        } else
            return tipComp;
    }

    @Override
    public String toString() {
        return appendTo(new StringBuilder(ATTEMPT)).toString();
    }

    public static StaffAttemptID forName(String str)
            throws IllegalArgumentException {
        if (str == null)
            return null;
        try {
            String[] parts = str.split(Character.toString(SEPARATOR));
            if (parts.length == 5) {
                if (parts[0].equals(ATTEMPT)) {
                    return new StaffAttemptID(parts[1], Integer
                            .parseInt(parts[2]), Integer.parseInt(parts[3]),
                            Integer.parseInt(parts[4]));
                }
            }
        } catch (Exception ex) {
            // fall below
        }
        throw new IllegalArgumentException("StaffAttemptId string : " + str
                + " is not properly formed");
    }

}
