/**
 * CopyRight by Chinamobile
 * 
 * Browse.java
 */
package com.chinamobile.bcbsp.fault.browse;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.chinamobile.bcbsp.fault.storage.Fault;

/**
 * 
 * 
 * the browse reduce the List<fault> that can use in the jsp --list List<Fault>
 * sortrecords = new ArrayList<Fault>();
 * 
 * 
 * 
 */
public class Browse {

    // private String Path = null;
    public ReadFaultlog readFaultlog = null;
    public SortList<Fault> sortFault = new SortList<Fault>();
    public List<Fault> sortrecords = new ArrayList<Fault>();

    boolean recordDefaultflag = false;
    boolean recordflag = false;

    Date before = null;
    Date now = null;
    long bufferedTime = 30000;
    long time = 0;

    // -----------------retrieveByLevel(int month);--------------------

    public Browse() {
        String BCBSP_HOME = System.getenv("BCBSP_HOME");
        String FaultStoragePath = BCBSP_HOME + "/logs/faultlog";
        readFaultlog = new ReadFaultlog(FaultStoragePath, getHdfshostName());
    }

    /**
     * get the record location futhur more to get the faultlog in localdisk or
     * hdfs
     * 
     * @return
     */
    public String getHdfshostName() {
        Configuration conf = new Configuration(false);
        String HADOOP_HOME = System.getenv("HADOOP_HOME");
        String corexml = HADOOP_HOME + "/conf/core-site.xml";
        conf.addResource(new Path(corexml));
        String hdfsNamenodehostName = conf.get("fs.default.name");
        return hdfsNamenodehostName;
    }

    /**
     * get the list of Fault ordered by Level
     */
    public List<Fault> retrieveByLevel() {

        if (!recordDefaultflag) {
            sortrecords = readFaultlog.read();
            before = new Date();
            recordDefaultflag = true;
        } else {
            now = new Date();
            time = now.getTime() - before.getTime();
            if (time > bufferedTime) {
                sortrecords = readFaultlog.read();
                before = new Date();
            }
        }
        sortFault.Sort(sortrecords, "getLevel", null);
        return sortrecords;
    }

    /**
     * get the list of Fault ordered by Worker
     */
    public List<Fault> retrieveByPosition() {

        if (!recordDefaultflag) {
            sortrecords = readFaultlog.read();
            before = new Date();
            recordDefaultflag = true;
        } else {
            now = new Date();
            time = now.getTime() - before.getTime();
            if (time > bufferedTime) {
                sortrecords = readFaultlog.read();
                before = new Date();
            }
        }
        sortFault.Sort(sortrecords, "getWorkerNodeName", null);
        return sortrecords;
    }

    public List<Fault> retrieveByType() {

        if (!recordDefaultflag) {
            sortrecords = readFaultlog.read();
            before = new Date();
            recordDefaultflag = true;
        } else {
            now = new Date();
            time = now.getTime() - before.getTime();
            if (time > bufferedTime) {
                sortrecords = readFaultlog.read();
                before = new Date();
            }
        }
        sortFault.Sort(sortrecords, "getType", null);
        return sortrecords;
    }

    public List<Fault> retrieveByTime() {

        if (!recordDefaultflag) {
            sortrecords = readFaultlog.read();
            before = new Date();
            recordDefaultflag = true;
        } else {
            now = new Date();
            time = now.getTime() - before.getTime();
            if (time > bufferedTime) {
                sortrecords = readFaultlog.read();
                before = new Date();
            }
        }
        sortFault.Sort(sortrecords, "getTimeOfFailure", "desc");
        return sortrecords;
    }

    /**
  *   get the list of Fault ordered by Level in monthnum month
	 */
    public List<Fault> retrieveByLevel(int monthnum) {
        recordDefaultflag = false;
        sortrecords = readFaultlog.read(monthnum);

        sortFault.Sort(sortrecords, "getLevel", null);
        return sortrecords;
    }

    public List<Fault> retrieveByPosition(int monthnum) {

        recordDefaultflag = false;
        sortrecords = readFaultlog.read(monthnum);
        sortFault.Sort(sortrecords, "getWorkerNodeName", null);
        return sortrecords;
    }

    public List<Fault> retrieveByType(int monthnum) {

        recordDefaultflag = false;
        sortrecords = readFaultlog.read(monthnum);
        sortFault.Sort(sortrecords, "getType", null);
        return sortrecords;
    }

    public List<Fault> retrieveByTime(int monthnum) {

        recordDefaultflag = false;
        sortrecords = readFaultlog.read(monthnum);
        sortFault.Sort(sortrecords, "getTimeOfFailure", "desc");
        return sortrecords;
    }

    /**
     * 
     * @param FaultLevel
     *            retrieve by one key level and sort by Level;
     * @return
     */
    public List<Fault> retrieveByLevel(String faultLevel) {

        recordDefaultflag = false;
        String[] keys = { faultLevel };
        sortrecords = readFaultlog.read(keys);
        sortFault.Sort(sortrecords, "getLevel", null);
        return sortrecords;

    }

    public List<Fault> retrieveByPosition(String faultPostion) {

        recordDefaultflag = false;
        String[] keys = { faultPostion };
        sortrecords = readFaultlog.read(keys);
        sortFault.Sort(sortrecords, "getWorkerNodeName", null);
        return sortrecords;
    }

    public List<Fault> retrieveByType(String faultType) {

        recordDefaultflag = false;
        String[] keys = { faultType };
        sortrecords = readFaultlog.read(keys);
        sortFault.Sort(sortrecords, "getType", null);
        return sortrecords;
    }

    public List<Fault> retrieveByTime(String faultOccurTime) {

        recordDefaultflag = false;
        String[] keys = { faultOccurTime };
        sortrecords = readFaultlog.read(keys);
        sortFault.Sort(sortrecords, "getTimeOfFailure", "desc");
        return sortrecords;
    }

    /**
     * retrieve by one key level and sort by Level,in monthnum month;
     * @param faultLevel
     * @param monthnum
     * @return
     */
    public List<Fault> retrieveByLevel(String faultLevel, int monthnum) {

        recordDefaultflag = false;
        String[] keys = { faultLevel };
        sortrecords = readFaultlog.read(keys, monthnum);
        sortFault.Sort(sortrecords, "getLevel", null);
        return sortrecords;

    }

    public List<Fault> retrieveByPosition(String faultPostion, int monthnum) {

        recordDefaultflag = false;
        String[] keys = { faultPostion };
        sortrecords = readFaultlog.read(keys, monthnum);
        sortFault.Sort(sortrecords, "getWorkerNodeName", null);
        return sortrecords;
    }

    public List<Fault> retrieveByType(String faultType, int monthnum) {

        recordDefaultflag = false;
        String[] keys = { faultType };
        sortrecords = readFaultlog.read(keys, monthnum);
        sortFault.Sort(sortrecords, "getType", null);
        return sortrecords;
    }

    public List<Fault> retrieveByTime(String faultOccurTime, int monthnum) {

        recordDefaultflag = false;
        String[] keys = { faultOccurTime };
        sortrecords = readFaultlog.read(keys, monthnum);
        sortFault.Sort(sortrecords, "getTimeOfFailure", "desc");
        return sortrecords;
    }

    /**
     * retrieve by some keys  and sort by time default, in monthnum month;
     * @param keys
     * @param monthnum
     * @return
     */
    public List<Fault> retrieveWithMoreKeys(String[] keys, int monthnum) {

        recordDefaultflag = false;
        sortrecords = readFaultlog.read(keys, monthnum);
        sortFault.Sort(sortrecords, "getTimeOfFailure", null);
        return sortrecords;
    }

    /**
     * retrieve by some keys  and sort by time default
     * @param keys
     * @return
     */
    public List<Fault> retrieveWithMoreKeys(String[] keys) {

        recordDefaultflag = false;
        sortrecords = readFaultlog.read(keys);
        sortFault.Sort(sortrecords, "getTimeOfFailure", null);
        return sortrecords;
    }


    public long getBufferedTime() {
        return bufferedTime;
    }

    public void setBufferedTime(long bufferedTime) {
        this.bufferedTime = bufferedTime;
    }

}
