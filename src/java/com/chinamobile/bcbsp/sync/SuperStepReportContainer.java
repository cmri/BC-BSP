/**
 * CopyRight by Chinamobile
 * 
 * SuperStepReportContainer.java
 */
package com.chinamobile.bcbsp.sync;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.chinamobile.bcbsp.Constants;

/**
 *  SuperStepReportContainer
 *  
 *  This class is a container which includes all information
 *  used during SuperStep Synchronization.
 *  Such as the aggregation values and the synchronization stage.
 * 
 * @author root
 *
 */
public class SuperStepReportContainer implements Writable {
    private int stageFlag = 0;
    private String[] dirFlag;
    private long judgeFlag = 0;

    private int checkNum = 0;
    private int partitionId = 0;
    
    private int localBarrierNum = 0;

    // Just for PARTITION_TYPE.RANGE
    private int maxRange = 0;
    private int minRange = 0;
    private int numCopy = 0;
    private HashMap<Integer,Integer> counter = new HashMap<Integer,Integer>();
    private int port1 = 60000;
    private int port2 = 60001;
    
    private String activeMQWorkerNameAndPorts;
    
    // For aggregation values
    private String[] aggValues;

    public SuperStepReportContainer() {
        this.dirFlag = new String[0];
        this.aggValues = new String[0];
    }

    public SuperStepReportContainer(int stageFlag, String[] dirFlag,
            long judgeFlag) {
        this.stageFlag = stageFlag;
        this.dirFlag = dirFlag;
        this.judgeFlag = judgeFlag;
        this.aggValues = new String[0];
    }
    
    public SuperStepReportContainer(int stageFlag, String[] dirFlag,
            long judgeFlag, String[] aggValues) {
        this.stageFlag = stageFlag;
        this.dirFlag = dirFlag;
        this.judgeFlag = judgeFlag;
        this.aggValues = aggValues;
    }

    // For transportation through the ZooKeeper in the synchronization process.
    public SuperStepReportContainer(String s) {
        if (s.equals("RECOVERY")) {
            return;
        }
        String[] content = s.split(Constants.SPLIT_FLAG);
        
        this.judgeFlag = Integer.valueOf(content[0]);
        
        /** 
         * To decapsulate the aggValues after the judgeFlag.
         * The content of the string should be in the form as follows:
         * ["judgeFlag:aggName1\taggValue:aggName2\taggValue:...:aggNameN\taggValueN"]
         */
        int count = content.length - 1; // Subtract one for the judgeFlag.
        this.aggValues = new String[count];
        for (int i = 0; i < count; i ++) {
            this.aggValues[i] = content[i+1];
        }
    }

    public void setActiveMQWorkerNameAndPorts(String str) {
        this.activeMQWorkerNameAndPorts = str;
    }
    
    public String getActiveMQWorkerNameAndPorts() {
        return this.activeMQWorkerNameAndPorts;
    }
    
    public int getLocalBarrierNum() {
        return localBarrierNum;
    }

    public void setLocalBarrierNum(int localBarrierNum) {
        this.localBarrierNum = localBarrierNum;
    }
    
    public void setStageFlag(int stageFlag) {
        this.stageFlag = stageFlag;
    }

    public int getStageFlag() {
        return this.stageFlag;
    }

    public void setDirFlag(String[] dirFlag) {
        this.dirFlag = dirFlag;
    }

    public String[] getDirFlag() {
        return this.dirFlag;
    }

    public void setJudgeFlag(long judgeFlag) {
        this.judgeFlag = judgeFlag;
    }

    public long getJudgeFlag() {
        return this.judgeFlag;
    }

    public void setCheckNum(int checkNum) {
        this.checkNum = checkNum;
    }

    public int getCheckNum() {
        return this.checkNum;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public int getPartitionId() {
        return this.partitionId;
    }

    public int getPort1() {
        return port1;
    }

    public void setPort1(int port) {
        this.port1 = port;
    }
    
    public int getPort2() {
        return port2;
    }
    
    public void setPort2(int port) {
        this.port2 = port;
    }
    
    public void setMaxRange(int maxRange) {
        this.maxRange = maxRange;
    }

    public int getMaxRange() {
        return this.maxRange;
    }

    public void setMinRange(int minRange) {
        this.minRange = minRange;
    }

    public int getMinRange() {
        return this.minRange;
    }

    public String[] getAggValues() {
        return aggValues;
    }

    public void setAggValues(String[] aggValues) {
        this.aggValues = aggValues;
    }
    public HashMap<Integer, Integer> getCounter() {
        return counter;
    }

    public void setCounter(HashMap<Integer, Integer> counter) {
        this.counter = counter;
    }
    
    public int getNumCopy() {
        return numCopy;
    }

    public void setNumCopy(int numCopy) {
        this.numCopy = numCopy;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.partitionId = in.readInt();
        this.stageFlag = in.readInt();
        int count = in.readInt();
        this.dirFlag = new String[count];
        for (int i = 0; i < count; i++) {
            this.dirFlag[i] = Text.readString(in);
        }
        this.judgeFlag = in.readLong();
        this.localBarrierNum = in.readInt();
        this.port1 = in.readInt();
        this.port2 = in.readInt();
        // For aggregation values
        count = in.readInt();
        this.aggValues = new String[count];
        for (int i = 0; i < count; i++) {
            this.aggValues[i] = Text.readString(in);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.partitionId);
        out.writeInt(this.stageFlag);
        out.writeInt(this.dirFlag.length);
        int count = this.dirFlag.length;
        for (int i = 0; i < count; i++) {
            Text.writeString(out, this.dirFlag[i]);
        }
        out.writeLong(this.judgeFlag);
        out.writeInt(this.localBarrierNum);
        out.writeInt(this.port1);
        out.writeInt(this.port2);
        // For aggregation values
        out.writeInt(this.aggValues.length);
        count = this.aggValues.length;
        for (int i = 0; i < count; i++) {
            Text.writeString(out, this.aggValues[i]);
        }
    }

    // For transportation through the ZooKeeper in the synchronization process.
    @Override
    public String toString() {
        String content = Long.toString(this.judgeFlag);
        
        /** 
         * To encapsulate the aggValues after the judgeFlag.
         * The content of the string should be in the form as follows:
         * ["judgeFlag:aggName1\taggValue:aggName2\taggValue:...:aggNameN\taggValueN"]
         */
        for (int i = 0; i < this.aggValues.length; i ++) {
            content = content + Constants.SPLIT_FLAG + this.aggValues[i];
        }
        
        return content;
    }
}
