/**
 * CopyRight by Chinamobile
 * 
 * Fault.java
 */
package com.chinamobile.bcbsp.fault.storage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.chinamobile.bcbsp.util.BSPJobID;

public class Fault implements Writable, Cloneable {
    static final Log LOG = LogFactory.getLog(Fault.class);

    public static enum Type {
        WORKERNODE, DISK, SYSTEMSERVICE, NETWORK,FORCEQUIT
    }

    public static enum Level {
        INDETERMINATE, WARNING, MINOR, MAJOR, CRITICAL
    }

    protected static final String DEFAULT_DATE_TIME_FORMAT = "yyyy/MM/dd HH:mm:ss,SSS";
    private static SimpleDateFormat dateFormat = new SimpleDateFormat(
            DEFAULT_DATE_TIME_FORMAT);

    private Type type = Type.DISK;
    private Level level = Level.CRITICAL;
    private String timeOfFailure = "";
    private String workerNodeName = "";
    private String jobName = "";
    private String staffName = "";
    private String exceptionMessage = "";
    private boolean faultStatus = true;
    private int superStep_Stage=0;

    public Fault() {
    }

    /**
     * The order of parameter :Type Level ,workerName
     * ,ExceptionMessage,JobName,StaffName.null maybe used when one or more
     * parameter is not needed;
     * 
     * @param type
     *            ENUM :Fault.Type.DISK
     * @param level
     *            ENUM
     * @param workerNodeName
     *            String
     * @param exceptionMessage
     *            String
     * @param jobName
     *            String
     * @param staffName
     *            String
     */
    /**
     *  used for staff without super step stage 
     */
    public Fault(Type type, Level level, String workerNodeName,
            String exceptionMessage, String jobName, String staffName) {
        this(type, level, workerNodeName, exceptionMessage, jobName, staffName,
               0);
    }
    /**
     *  used for workernode
     * @param type
     * @param level
     * @param workerNodeName
     * @param exceptionMessage
     */
    public Fault(Type type, Level level, String workerNodeName,
            String exceptionMessage) {
        this(type, level, workerNodeName, exceptionMessage, "null", "null",
               0);
    }

    public Fault(Type type, Level level, String workerNodeName,
            String exceptionMessage, String jobName, String staffName
            ,int superStep_Stage ) {
        this.type = type;
        this.level = level;
        this.timeOfFailure = dateFormat.format(new Date());
        this.jobName = jobName;
        this.staffName = staffName;
        this.exceptionMessage = exceptionMessage;
        this.workerNodeName = workerNodeName;
        this.superStep_Stage=superStep_Stage;
    }

    public Fault(Type type, Level level, BSPJobID jobID,
            String exceptionMessage) {
        this.type = type;
        this.level = level;
        this.timeOfFailure = dateFormat.format(new Date());
        this.jobName = jobID.toString();
        this.exceptionMessage = exceptionMessage;
        
    }
    
    public int getSuperStep_Stage() {
        return superStep_Stage;
    }

    public void setSuperStep_Stage(int superStep_Stage) {
        this.superStep_Stage = superStep_Stage;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public void setLevel(Level level) {
        this.level = level;
    }

    public void setTimeOfFailure(String timeOfFailure) {
        this.timeOfFailure = timeOfFailure;
    }

    public void setExceptionMessage(String exceptionMessage) {
        this.exceptionMessage = exceptionMessage;
    }

    public Type getType() {
        return type;
    }

    public Level getLevel() {
        return level;
    }

    public String getTimeOfFailure() {
        return timeOfFailure;
    }

    public String getWorkerNodeName() {
        return workerNodeName;
    }

    public void setWorkerNodeName(String workerNodeName) {
        this.workerNodeName = workerNodeName;
    }

    public String getJobName() {
        return jobName;
    }

    public String getStaffName() {
        return staffName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public void setStaffName(String staffName) {
        this.staffName = staffName;
    }

    public void setFaultStatus(boolean faultStatus) {
        this.faultStatus = faultStatus;
    }

    public boolean isFaultStatus() {
        return faultStatus;
    }

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    public void write(DataOutput out) throws IOException {
        WritableUtils.writeEnum(out, this.type);
        WritableUtils.writeEnum(out, this.level);

        Text.writeString(out, this.timeOfFailure);
        Text.writeString(out, this.workerNodeName);
        Text.writeString(out, this.jobName);
        Text.writeString(out, this.staffName);
        Text.writeString(out, this.exceptionMessage);
        out.writeInt(this.superStep_Stage);
    }

    public void readFields(DataInput in) throws IOException {
        this.type = WritableUtils.readEnum(in, Type.class);
        this.level = WritableUtils.readEnum(in, Level.class);

        this.timeOfFailure = Text.readString(in);
        this.workerNodeName = Text.readString(in);
        this.jobName = Text.readString(in);
        this.staffName = Text.readString(in);
        this.exceptionMessage = Text.readString(in);
        this.superStep_Stage = in.readInt();
    }

    @Override
    public String toString() {
        return this.timeOfFailure + "--" + this.type.toString() + "--"
                + this.level + "--" + this.workerNodeName + "--" + this.jobName
                + "--" + this.staffName + "--" + this.exceptionMessage + "--"
                + this.faultStatus+"--" + this.superStep_Stage;
    }

    public boolean equals(Object obj){
        Fault fault = (Fault)obj;
        if(this.timeOfFailure.equals(fault.timeOfFailure)&&this.workerNodeName.equals(fault.workerNodeName)){
            return true;
        }else{
            return false;
        }
    
        }
    
    public int hashcode(){
        return timeOfFailure.hashCode()+workerNodeName.hashCode();
    }
    
    public Fault clone() throws CloneNotSupportedException {
        Fault fault;
        fault=(Fault)super.clone();
        return fault;
    }
}
