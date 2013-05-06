/**
 * CopyRight by Chinamobile
 * 
 * SuperStepCommand.java
 */
package com.chinamobile.bcbsp.sync;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.chinamobile.bcbsp.Constants;

/**
 * SuperStepCommand
 *
 * SuperStepCommand maintains the actions which JobInProgress generates and the
 * aggregation information
 * 
 * @author
 * @version
 */

public class SuperStepCommand implements Writable {
    
    private static final Log LOG = LogFactory.getLog(SuperStepCommand.class);
    
    private int commandType;
    private String initWritePath = "initWritePath";
    private String initReadPath = "initReadPath";
    private int ableCheckPoint = 0;
    private int nextSuperStepNum = 0;
    private int oldCheckPoint = 0;
    private HashMap<Integer, String> partitionToWorkerManagerNameAndPort = null;
    
    // For aggregation values
    private String[] aggValues;

    public SuperStepCommand() {
        this.aggValues = new String[0];
        this.partitionToWorkerManagerNameAndPort = new HashMap<Integer, String>();
    }

    public SuperStepCommand(int commandType) {
        this.commandType = commandType;
    }

    public SuperStepCommand(int commandType, String initWritePath,
            String initReadPath, int ableCheckPoint, int nextSuperStepNum) {
        this.commandType = commandType;
        this.initWritePath = initWritePath;
        this.initReadPath = initReadPath;
        this.ableCheckPoint = ableCheckPoint;
        this.nextSuperStepNum = nextSuperStepNum;
    }
    
    public SuperStepCommand(int commandType, String initWritePath,
            String initReadPath, int ableCheckPoint, int nextSuperStepNum,
            String[] aggValues) {
        this.commandType = commandType;
        this.initWritePath = initWritePath;
        this.initReadPath = initReadPath;
        this.ableCheckPoint = ableCheckPoint;
        this.nextSuperStepNum = nextSuperStepNum;
        this.aggValues = aggValues;
    }

    public SuperStepCommand(String s) {
        String[] tmp = s.split(Constants.SSC_SPLIT_FLAG);
        int length = tmp.length;
        int index = 0;
        
        if (length < 6) {
        } else {
        	this.commandType = Integer.valueOf(tmp[index++]);
            this.initWritePath = tmp[index++];
            this.initReadPath = tmp[index++];
            this.ableCheckPoint = Integer.valueOf(tmp[index++]);
            this.nextSuperStepNum = Integer.valueOf(tmp[index++]);
            this.oldCheckPoint = Integer.valueOf(tmp[index++]);
        }
        
        LOG.info("[SuperStepCommand]--[index]" + index);
        LOG.info("[SuperStepCommand]--[CommandType]" + this.commandType);
        
        if (this.commandType == Constants.COMMAND_TYPE.START_AND_RECOVERY) {
        	String str = tmp[index++];//{1=a, 2=b, 3=c}
        	LOG.info("[SuperStepCommand]--[routeString]" + str);
            //remove "{" and "}"
            String regEx = "[\\{\\}]";
            Pattern p = Pattern.compile(regEx);
            Matcher m = p.matcher(str);
            str = m.replaceAll("");//1=a, 2=b, 3=c
            str = str.replace(" ", "");//1=a,2=b,3=c

            this.partitionToWorkerManagerNameAndPort = new HashMap<Integer, String>();
            String[] strMap = str.split(",");//1=a|2=b|3=c
            for(int i=0; i<strMap.length; i++) {
                String[] strKV = strMap[i].split("=");//1 a | 2 b | 3 c
                if(strKV.length == 2) {
                    this.partitionToWorkerManagerNameAndPort.put(Integer.parseInt(strKV[0]), strKV[1]); 
                } else {
                }
            }	
        }
        
        if (index < length) {
        	/** 
             * To decode the aggValues after the judgeFlag.
             * The content of the string should be in the form as follows:
             * ["judgeFlag:aggName1\taggValue:aggName2\taggValue:...:aggNameN\taggValueN"]
             */
            int count = length -index; // Subtract 5 for above attributes.
            this.aggValues = new String[count];
            for (int i = 0; i < count; i ++) {
                this.aggValues[i] = tmp[index++];
            }
        }
    }

    public int getOldCheckPoint() {
        return oldCheckPoint;
    }

    public void setOldCheckPoint(int oldCheckPoint) {
        this.oldCheckPoint = oldCheckPoint;
    }

    /**
     * Set the Type of Command.
     * 
     * @param commandType
     */
    public void setCommandType(int commandType) {
        this.commandType = commandType;
    }

    /**
     * Get the Type of Command.
     * 
     * @return
     */
    public int getCommandType() {
        return this.commandType;
    }

    /**
     * Set the initial path for writing CheckPoint.
     * 
     * @param initWritePath
     */
    public void setInitWritePath(String initWritePath) {
        this.initWritePath = initWritePath;
    }

    /**
     * Get the initial path for writing CheckPoint.
     * 
     * @return
     */
    public String getInitWritePath() {
        return this.initWritePath;
    }

    /**
     * Set the initial path for loading CheckPoint.
     * 
     * @param initReadPath
     */
    public void setInitReadPath(String initReadPath) {
        this.initReadPath = initReadPath;
    }

    /**
     * Get the initial path for loading CheckPoint.
     * 
     * @return
     */
    public String getInitReadPath() {
        return this.initReadPath;
    }

    /**
     * Set the last available CheckPoint.
     * the available CheckPoint is denoted by the SuperStepCounter.
     * 
     * @param ableCheckPoint
     */
    public void setAbleCheckPoint(int ableCheckPoint) {
        this.ableCheckPoint = ableCheckPoint;
    }

    /**
     * Get the last available CheckPoint.
     * 
     * @return
     */
    public int getAbleCheckPoint() {
        return this.ableCheckPoint;
    }

    /**
     * Set the next SuperStepCounter.
     * All staffs will read it and set themselves SuperStepCounter.
     * 
     * @param nextSuperStepNum
     */
    public void setNextSuperStepNum(int nextSuperStepNum) {
        this.nextSuperStepNum = nextSuperStepNum;
    }

    /**
     * Get the next SuperStepCounter.
     * 
     * @return
     */
    public int getNextSuperStepNum() {
        return this.nextSuperStepNum;
    }

    /**
     * Set the aggregate values.
     * 
     * @param aggValues the aggValues to set
     */
    public void setAggValues(String[] aggValues) {
        this.aggValues = aggValues;
    }

    /**
     * Get the aggregate values.
     * 
     * @return the aggValues
     */
    public String[] getAggValues() {
        return aggValues;
    }

    public void setPartitionToWorkerManagerNameAndPort(
            HashMap<Integer, String> partitionToWorkerManagerNameAndPort) {
        this.partitionToWorkerManagerNameAndPort = partitionToWorkerManagerNameAndPort;
    }

    public HashMap<Integer, String> getPartitionToWorkerManagerNameAndPort() {
        return partitionToWorkerManagerNameAndPort;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        this.commandType = in.readInt();
        this.initWritePath = Text.readString(in);
        this.initReadPath = Text.readString(in);
        this.ableCheckPoint = in.readInt();
        this.nextSuperStepNum = in.readInt();
        this.oldCheckPoint = in.readInt();
        // For aggregation values
        int count = in.readInt();
        this.aggValues = new String[count];
        for (int i = 0; i < count; i++) {
            this.aggValues[i] = Text.readString(in);
        }
        //nc
        int size = WritableUtils.readVInt(in);
        if(size > 0) {
            String[] partitionToWMName = WritableUtils.readCompressedStringArray(in);
            this.partitionToWorkerManagerNameAndPort = new HashMap<Integer, String>();
            for(int j=0; j<size; j++) {
                this.partitionToWorkerManagerNameAndPort.put(j, partitionToWMName[j]);
            }   
        }//
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.commandType);
        Text.writeString(out, this.initWritePath);
        Text.writeString(out, this.initReadPath);
        out.writeInt(this.ableCheckPoint);
        out.writeInt(this.nextSuperStepNum);
        out.writeInt(this.oldCheckPoint);
        // For aggregation values
        out.writeInt(this.aggValues.length);
        int count = this.aggValues.length;
        for (int i = 0; i < count; i++) {
            Text.writeString(out, this.aggValues[i]);
        }
        
        //nc
        if(partitionToWorkerManagerNameAndPort == null)  {
            WritableUtils.writeVInt(out, 0);
        } else {
            WritableUtils.writeVInt(out, partitionToWorkerManagerNameAndPort.size());
            String[] partitionToWMName = null;
            for(Integer i : this.partitionToWorkerManagerNameAndPort.keySet()) {
                partitionToWMName[i] = partitionToWorkerManagerNameAndPort.get(i);
            }
            WritableUtils.writeCompressedStringArray(out, partitionToWMName);
        }//
    }

    @Override
    public String toString() {
        
        String content = this.commandType + Constants.SSC_SPLIT_FLAG
                + this.initWritePath + Constants.SSC_SPLIT_FLAG + this.initReadPath
                + Constants.SSC_SPLIT_FLAG + this.ableCheckPoint
                + Constants.SSC_SPLIT_FLAG + this.nextSuperStepNum
                + Constants.SSC_SPLIT_FLAG + this.oldCheckPoint;
        
        if(this.commandType == Constants.COMMAND_TYPE.START_AND_RECOVERY) {
        	if (this.partitionToWorkerManagerNameAndPort == null) {
        		LOG.error("This partitionToWorkerManagerNameAndPort is null");
        	} else {
        	    content = content + Constants.SSC_SPLIT_FLAG + this.partitionToWorkerManagerNameAndPort;
        	}
        }
        
        /** 
         * To encapsulate the aggValues after the judgeFlag.
         * The content of the string should be in the form as follows:
         * ["judgeFlag:aggName1\taggValue:aggName2\taggValue:...:aggNameN\taggValueN"]
         */
        for (int i = 0; i < this.aggValues.length; i ++) {
            content = content + Constants.SSC_SPLIT_FLAG + this.aggValues[i];
        }
        
        return content;
    }  
}
