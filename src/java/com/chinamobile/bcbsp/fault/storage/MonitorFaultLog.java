/**
 * CopyRight by Chinamobile
 * 
 * MonitorFaultLog.java
 */
package com.chinamobile.bcbsp.fault.storage;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;



public class MonitorFaultLog {

    /**
     *    default constructor  the fault log is stored in  logs of bsp root directory
     */
    
    public static final Log LOG = LogFactory.getLog(MonitorFaultLog.class);
    
    public MonitorFaultLog() {
        this(getFaultStoragePath(), getFaultStoragePath());
    }

    public MonitorFaultLog(String localFaultDir) {

        this(localFaultDir, localFaultDir);
    }

    private static String getFaultStoragePath() {

        String BCBSP_HOME = System.getenv("BCBSP_HOME");
        String FaultStoragePath = BCBSP_HOME + "/logs/faultlog/";
        return FaultStoragePath;

    }

    /**
     * 
     * @param localDirPath 
     * @param hdfsDir
     * ManageFaultLog is used to manage the storage of fault log
     */
    private MonitorFaultLog(String localDirPath, String hdfsDir) {
        String hdfsNamenodehostName = getHdfsNameNodeHostName();
        if (!localDirPath.substring(localDirPath.length() - 1).equals("/")
                && !localDirPath.substring(localDirPath.length() - 2).equals(
                        "\\")) {
            localDirPath = localDirPath + File.separator;
        }
        if (!hdfsDir.substring(hdfsDir.length() - 1).equals("/")
                && !hdfsDir.substring(hdfsDir.length() - 2).equals("\\")) {
            hdfsDir = hdfsDir + File.separator;
        }
        this.localDirPath = localDirPath;
        this.domainName = hdfsNamenodehostName;
        this.hdfsDir = hdfsDir;
        this.mfl = new ManageFaultLog(domainName, hdfsDir);
    }

   private String getHdfsNameNodeHostName() {
        Configuration conf = new Configuration(false);
        String HADOOP_HOME = System.getenv("HADOOP_HOME");
        String corexml = HADOOP_HOME + "/conf/core-site.xml";
        conf.addResource(new Path(corexml));
        String hdfsNamenodehostName = conf.get("fs.default.name");
        return hdfsNamenodehostName;
    }

    /** The default format to use when formating dates */
    static protected final String DEFAULT_DATE_TIME_FORMAT = "yyyy/MM/dd HH:mm:ss,SSS";
    static protected TimeZone timezone = null;

    static protected String storagePath = null;
    static private PrintWriter out = null;

    private String localDirPath = null;
    private String domainName = null;
    private String hdfsDir = null;

    private ManageFaultLog mfl = null;
    protected int num = 0;

    /**
     * 
     * @param fault record some kinds fault message 
     * record the fault message into specified file
     */
    public void faultLog(Fault fault) {

        StringBuffer buf = new StringBuffer();

        buf.append(fault.getTimeOfFailure());
        buf.append(" -- ");

        buf.append(fault.getType() );
        buf.append(" -- ");

        buf.append(fault.getLevel());
        buf.append(" -- ");

        buf.append(fault.getWorkerNodeName());
        buf.append(" -- ");

        if (fault.getJobName() != "" && fault.getJobName() != null) {
            buf.append(fault.getJobName());
            buf.append(" -- ");
        }else{
            buf.append("null");
            buf.append(" -- ");
        }

        if (fault.getStaffName() != "" && fault.getStaffName() != null) {
            buf.append(fault.getStaffName());
            buf.append(" -- ");
        }else{
            buf.append("null");
            buf.append(" -- ");
        }
        
        if (fault.getExceptionMessage() != ""
                && fault.getExceptionMessage() != null) {
            buf.append(" [");
            buf.append(fault.getExceptionMessage());
            buf.append("]");
            buf.append(" -- ");
        }else{
            buf.append("null");
            buf.append(" -- ");
        }
        
        buf.append(fault.isFaultStatus());
        write(buf);
    }

    /**
     * 
     * @param buffer
     *  according fault time to create directory and write fault file 
     */
    protected void write(StringBuffer buffer) {

        Date now = new Date(System.currentTimeMillis());
        timezone = TimeZone.getTimeZone("GMT+08:00");
        Calendar currentTime = Calendar.getInstance();
        currentTime.setTimeZone(timezone);
        currentTime.setTime(now);
        String dateText = null;
        String logFileName = "faultLog.txt";
        String YEAR = String.valueOf(currentTime.get(Calendar.YEAR));
        String MONTH = String.valueOf(currentTime.get(Calendar.MONTH) + 1);
        String DAY = String.valueOf(currentTime.get(Calendar.DAY_OF_MONTH));
        dateText = YEAR + "/" + MONTH + "/" + DAY + "--";
        storagePath = localDirPath + dateText + logFileName;
        write(buffer, storagePath);

    }

    /**
     *    write log and manage fault directory
     * @param buffer
     * @param path
     */
    protected void write(StringBuffer buffer, String filePath) {

        try {

            File f = new File(filePath);
            if (!f.getParentFile().exists()) {
                f.getParentFile().mkdirs();
                mfl.record(f.getParentFile());
            }
            out = new PrintWriter(new FileWriter(f, true));

            out.println(buffer.toString());
            out.flush();
            out.close();
        } catch (IOException e) {
            LOG.error("[write]", e);
        }

    }

    public String getDomainName() {
        return domainName;
    }

    public String getHdfsDir() {
        return hdfsDir;
    }

    public void setHdfsDir(String hdfsDir) {
        this.hdfsDir = hdfsDir;
    }

    public String getLocaldirpath() {
        return localDirPath;
    }

    public static String getStoragePath() {
        return storagePath;
    }

    public static void setStoragePath(String storagePath) {
        MonitorFaultLog.storagePath = storagePath;
    }
}
