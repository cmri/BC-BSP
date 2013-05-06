/**
 * CopyRight by Chinamobile
 * 
 * BSPJobClient.java
 */
package com.chinamobile.bcbsp.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.bspcontroller.BSPController;
import com.chinamobile.bcbsp.bspcontroller.ClusterStatus;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.rpc.JobSubmissionProtocol;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.JobProfile;
import com.chinamobile.bcbsp.util.JobStatus;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.util.StaffStatus;

/**
 * BSPJobClient
 * 
 * BSPJobClient is the primary interface for the user-job to interact with the
 * BSPController.
 * 
 * BSPJobClient provides facilities to submit jobs, track their progress, access
 * component-staffs' reports/logs, get the BC-BSP cluster status information
 * etc.
 * 
 * @author
 * @version
 */
public class BSPJobClient extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(BSPJobClient.class);

    public static enum StaffStatusFilter {
        NONE, KILLED, FAILED, SUCCEEDED, ALL
    }

    private static final long MAX_JOBPROFILE_AGE = 1000 * 2;

    public class NetworkedJob implements RunningJob {
        JobProfile profile;
        JobStatus status;
        long statustime;

        public NetworkedJob(JobStatus job) throws IOException {
            this.status = job;
            this.profile = jobSubmitClient.getJobProfile(job.getJobID());
            this.statustime = System.currentTimeMillis();
        }

        /**
         * Some methods rely on having a recent job profile object. Refresh it,
         * if necessary
         */
        synchronized void ensureFreshStatus() throws IOException {
            if (System.currentTimeMillis() - statustime > MAX_JOBPROFILE_AGE) {
                updateStatus();
            }
        }

        /**
         * Some methods need to update status immediately. So, refresh
         * immediately
         * 
         * @throws IOException
         */
        synchronized void updateStatus() throws IOException {
            this.status = jobSubmitClient.getJobStatus(profile.getJobID());
            this.statustime = System.currentTimeMillis();
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.chinamobile.bcbsp.bsp.RunningJob#getID()
         */
        @Override
        public BSPJobID getID() {
            return profile.getJobID();
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.chinamobile.bcbsp.bsp.RunningJob#getJobName()
         */
        @Override
        public String getJobName() {
            return profile.getJobName();
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.chinamobile.bcbsp.bsp.RunningJob#getJobFile()
         */
        @Override
        public String getJobFile() {
            return profile.getJobFile();
        }

        @Override
        public long progress() throws IOException {
            ensureFreshStatus();
            return status.progress();
        }

        @Override
        public boolean isComplete() throws IOException {
            updateStatus();
            return (status.getRunState() == JobStatus.SUCCEEDED
                    || status.getRunState() == JobStatus.FAILED || status
                    .getRunState() == JobStatus.KILLED);
        }

        @Override
        public boolean isSuccessful() throws IOException {
            return status.getRunState() == JobStatus.SUCCEEDED;
        }

        @Override
        public boolean isKilled() throws IOException {
            return status.getRunState() == JobStatus.KILLED;
        }

        @Override
        public boolean isFailed() throws IOException {
            return status.getRunState() == JobStatus.FAILED;
        }

        @Override
        public boolean isRecovery() throws IOException {
            return status.getRunState() == JobStatus.RECOVERY;
        }

        public synchronized long getSuperstepCount() throws IOException {
            ensureFreshStatus();
            return status.getSuperstepCount();
        }

        /**
         * Blocks until the job is finished
         */
        public void waitForCompletion() throws IOException {
            while (!isComplete()) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                }
            }
        }

        /**
         * Tells the service to get the state of the current job.
         */
        public synchronized int getJobState() throws IOException {
            updateStatus();
            return status.getRunState();
        }

        /**
         * Tells the service to terminate the current job.
         */
        public synchronized void killJob() throws IOException {
            jobSubmitClient.killJob(getID());
        }

        @Override
        public void killStaff(StaffAttemptID staffId, boolean shouldFail)
                throws IOException {
            jobSubmitClient.killStaff(staffId, shouldFail);
        }
    }

    private static class NewSplitComparator implements
            Comparator<org.apache.hadoop.mapreduce.InputSplit> {

        @Override
        public int compare(org.apache.hadoop.mapreduce.InputSplit o1,
                org.apache.hadoop.mapreduce.InputSplit o2) {
            try {
                long len1 = o1.getLength();
                long len2 = o2.getLength();
                if (len1 < len2) {
                    return 1;
                } else if (len1 == len2) {
                    return 0;
                } else {
                    return -1;
                }
            } catch (IOException ie) {
                throw new RuntimeException("exception in compare", ie);
            } catch (InterruptedException ie) {
                throw new RuntimeException("exception in compare", ie);
            }
        }
    }

    public static class RawSplit implements Writable {
        private String splitClass;
        private BytesWritable bytes = new BytesWritable();
        private String[] locations;
        long dataLength;

        public void setBytes(byte[] data, int offset, int length) {
            bytes.set(data, offset, length);
        }

        public void setClassName(String className) {
            splitClass = className;
        }

        public String getClassName() {
            return splitClass;
        }

        public BytesWritable getBytes() {
            return bytes;
        }

        public void clearBytes() {
            bytes = null;
        }

        public void setLocations(String[] locations) {
            this.locations = locations;
        }

        public String[] getLocations() {
            return locations;
        }

        public void readFields(DataInput in) throws IOException {
            splitClass = Text.readString(in);
            dataLength = in.readLong();
            bytes.readFields(in);
            int len = WritableUtils.readVInt(in);
            locations = new String[len];
            for (int i = 0; i < len; ++i) {
                locations[i] = Text.readString(in);
            }
        }

        public void write(DataOutput out) throws IOException {
            Text.writeString(out, splitClass);
            out.writeLong(dataLength);
            bytes.write(out);
            WritableUtils.writeVInt(out, locations.length);

            for (int i = 0; i < locations.length; i++) {
                Text.writeString(out, locations[i]);
            }
        }

        public long getDataLength() {
            return dataLength;
        }

        public void setDataLength(long l) {
            dataLength = l;
        }
    }

    private JobSubmissionProtocol jobSubmitClient = null;
    
    private Path sysDir = null;
    private FileSystem fs = null;

    // job files are world-wide readable and owner writable
    final private static FsPermission JOB_FILE_PERMISSION = FsPermission
            .createImmutable(( short ) 0644); // rw-r--r--

    // job submission directory is world readable/writable/executable
    final static FsPermission JOB_DIR_PERMISSION = FsPermission
            .createImmutable(( short ) 0777); // rwx-rwx-rwx

    public BSPJobClient(Configuration conf) throws IOException {
        setConf(conf);
        init(conf);
    }

    public BSPJobClient() {
    }

    public void init(Configuration conf) throws IOException {
        this.jobSubmitClient = ( JobSubmissionProtocol ) RPC.getProxy(
                JobSubmissionProtocol.class, JobSubmissionProtocol.versionID,
                BSPController.getAddress(conf), conf,
                NetUtils.getSocketFactory(conf, JobSubmissionProtocol.class));
    }

    /**
     * Close the <code>JobClient</code>.
     */
    public synchronized void close() throws IOException {
        RPC.stopProxy(jobSubmitClient);
    }

    /**
     * Get a filesystem handle. We need this to prepare jobs for submission to
     * the BSP system.
     * 
     * @return the filesystem handle.
     */
    public synchronized FileSystem getFs() throws IOException {
        if (this.fs == null) {
            Path sysDir = getSystemDir();
            this.fs = sysDir.getFileSystem(getConf());
        }
        return fs;
    }

    /**
     * Gets the jobs that are submitted.
     * 
     * @return array of {@link JobStatus} for the submitted jobs.
     * @throws IOException
     */
    public JobStatus[] getAllJobs() throws IOException {
        return jobSubmitClient.getAllJobs();
    }

    public JobSubmissionProtocol getJobSubmitClient() {
        return jobSubmitClient;
    }

    /**
     * Gets the jobs that are not completed and not failed.
     * 
     * @return array of {@link JobStatus} for the running/to-be-run jobs.
     * @throws IOException
     */
    public JobStatus[] jobsToComplete() throws IOException {
        return jobSubmitClient.jobsToComplete();
    }

    private UnixUserGroupInformation getUGI(Configuration conf)
            throws IOException {
        UnixUserGroupInformation ugi = null;
        try {
            ugi = UnixUserGroupInformation.login(conf, true);
        } catch (LoginException e) {
            throw ( IOException ) (new IOException(
                    "Failed to get the current user's information.")
                    .initCause(e));
        }
        return ugi;
    }

    /**
     * Submit a job to the BC-BSP system. This returns a handle to the
     * {@link RunningJob} which can be used to track the running-job.
     * 
     * @param job
     *            the job configuration.
     * @return a handle to the {@link RunningJob} which can be used to track the
     *         running-job.
     * @throws FileNotFoundException
     * @throws IOException
     */
    public RunningJob submitJob(BSPJob job) throws FileNotFoundException,
            ClassNotFoundException, InterruptedException, IOException {
        return submitJobInternal(job);
    }

    /**
     * Submit a new job to run.
     * @param job
     * @return
     * 
     * Review comments:
     *   (1)The content of submitJobDir is decided by the client. I think it
     *      is dangerous because two different clients maybe generate the same submitJobDir.
     * Review time: 2011-11-30;
     * Reviewer: Hongxu Zhang.
     * 
     * Fix log:
     *   (1)In order to avoid the conflict, I use the jobId to generate the submitJobDir.
     *      Because the jobId is unique so this problem can be solved.
     * Fix time: 2011-12-04;
     * Programmer: Zhigang Wang.
     * 
     * Review comments:
     *   (2)There, the client must submit relative information about the job. There
     *      maybe some exceptions during this process. When exceptions occur, this job
     *      should not be executed and the relative submitJobDir must be cleanup.
     * Review time: 2011-12-04;
     * Reviewer: Hongxu Zhang.
     * 
     * Fix log:
     *   (2)The process of submiting files has been surrounded by try-catch. The submitJobDir will be
     *      cleanup in the catch process.
     * Fix time: 2011-12-04;
     * Programmer: Zhigang Wang.
     */
    public RunningJob submitJobInternal(BSPJob job) {
        BSPJobID jobId = null;
        Path submitJobDir = null;
        try {
            jobId = jobSubmitClient.getNewJobId();
            submitJobDir = new Path(getSystemDir(), "submit_"
                    + jobId.toString());
            
            Path submitJarFile = new Path(submitJobDir, "job.jar");
            Path submitJobFile = new Path(submitJobDir, "job.xml");

            Path submitSplitFile = new Path(submitJobDir, "job.split");

            // set this user's id in job configuration, so later job files can
            // be accessed using this user's id
            UnixUserGroupInformation ugi = getUGI(job.getConf());

            // Create a number of filenames in the BSPController's fs namespace
            FileSystem fs = getFs();
            fs.delete(submitJobDir, true);
            submitJobDir = fs.makeQualified(submitJobDir);
            submitJobDir = new Path(submitJobDir.toUri().getPath());
            FsPermission bspSysPerms = new FsPermission(JOB_DIR_PERMISSION);
            FileSystem.mkdirs(fs, submitJobDir, bspSysPerms);
            fs.mkdirs(submitJobDir);
            short replication = ( short ) job.getInt("bsp.submit.replication", 10);

            String originalJarPath = job.getJar();

            if (originalJarPath != null) {
                // copy jar to BSPController's fs
                // use jar name if job is not named.
                if ("".equals(job.getJobName())) {
                    job.setJobName(new Path(originalJarPath).getName());
                }
                job.setJar(submitJarFile.toString());
                fs.copyFromLocalFile(new Path(originalJarPath), submitJarFile);

                fs.setReplication(submitJarFile, replication);
                fs.setPermission(submitJarFile, new FsPermission(
                        JOB_FILE_PERMISSION));
            } else {
                LOG.warn("No job jar file set.  User classes may not be found. "
                        + "See BSPJob#setJar(String) or check Your jar file.");
            }

            // Set the user's name and working directory
            job.setUser(ugi.getUserName());
            if (ugi.getGroupNames().length > 0) {
                job.set("group.name", ugi.getGroupNames()[0]);
            }
            if (job.getWorkingDirectory() == null) {
                job.setWorkingDirectory(fs.getWorkingDirectory());
            }
            
            int maxClusterStaffs = jobSubmitClient.getClusterStatus(false).getMaxClusterStaffs();
            if (job.getNumPartition() == 0) {
                job.setNumPartition(maxClusterStaffs);
            }
            if (job.getNumPartition() > maxClusterStaffs) {
                job.setNumPartition(maxClusterStaffs);
            }
            job.setNumBspStaff(job.getNumPartition());
            
            int splitNum = 0;
            splitNum = writeSplits(job, submitSplitFile);
            
            if (splitNum > job.getNumPartition() && splitNum <= maxClusterStaffs) {
                job.setNumPartition(splitNum);
                job.setNumBspStaff(job.getNumPartition());
            } 
            
            if (splitNum > maxClusterStaffs) {
                LOG.error("Sorry, the number of files is more than maxClusterStaffs:" +
                        maxClusterStaffs);
                throw new IOException("Could not launch job");
            }
            
            job.set(Constants.USER_BC_BSP_JOB_SPLIT_FILE,
                    submitSplitFile.toString());
            
            LOG.info("[Max Staff Number] " + maxClusterStaffs);
            LOG.info("The number of splits for the job is: " + splitNum);
            LOG.info("The number of staffs for the job is: " + job.getNumBspStaff());
            // Write job file to BSPController's fs
            FSDataOutputStream out = FileSystem.create(fs, submitJobFile,
                    new FsPermission(JOB_FILE_PERMISSION));

            try {
                job.writeXml(out);
            } finally {
                out.close();
            }
            
            // Now, actually submit the job (using the submit name)
            JobStatus status = jobSubmitClient.submitJob(jobId,
                    submitJobFile.toString());
            if (status != null) {
                return new NetworkedJob(status);
            } else {
                throw new IOException("Could not launch job");
            }

        } catch (FileNotFoundException fnfE) {
            LOG.error("Exception has been catched in BSPJobClient--submitJobInternal !", fnfE);
            Fault f = new Fault(Fault.Type.SYSTEMSERVICE,
                    Fault.Level.INDETERMINATE, "null", fnfE.toString());
            jobSubmitClient.recordFault(f);
            jobSubmitClient.recovery(jobId);
            try {
                FileSystem fs = getFs();
                fs.delete(submitJobDir, true);
            } catch (Exception e) {
                LOG.error("Failed to cleanup the submitJobDir:" + submitJobDir);
            }
            return null;
        } catch (ClassNotFoundException cnfE) {
            LOG.error("Exception has been catched in BSPJobClient--submitJobInternal !", cnfE);
            Fault f = new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.WARNING,
                    "null", cnfE.toString());
            jobSubmitClient.recordFault(f);
            jobSubmitClient.recovery(jobId);
            try {
                FileSystem fs = getFs();
                fs.delete(submitJobDir, true);
            } catch (Exception e) {
                LOG.error("Failed to cleanup the submitJobDir:" + submitJobDir);
            }
            return null;
        } catch (InterruptedException iE) {
            LOG.error("Exception has been catched in BSPJobClient--submitJobInternal !", iE);
            Fault f = new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.CRITICAL,
                    "null", iE.toString());
            jobSubmitClient.recordFault(f);
            jobSubmitClient.recovery(jobId);
            try {
                FileSystem fs = getFs();
                fs.delete(submitJobDir, true);
            } catch (Exception e) {
                LOG.error("Failed to cleanup the submitJobDir:" + submitJobDir);
            }
            return null;
        } catch (Exception ioE) {
            LOG.error("Exception has been catched in BSPJobClient--submitJobInternal !", ioE);
            Fault f = new Fault(Fault.Type.DISK, Fault.Level.CRITICAL,
                    "null", ioE.toString());
            jobSubmitClient.recordFault(f);
            jobSubmitClient.recovery(jobId);
            try {
                FileSystem fs = getFs();
                fs.delete(submitJobDir, true);
            } catch (Exception e) {
                LOG.error("Failed to cleanup the submitJobDir:" + submitJobDir);
            }
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends org.apache.hadoop.mapreduce.InputSplit> int writeSplits(
            BSPJob job, Path submitSplitFile) throws IOException,
            InterruptedException, ClassNotFoundException {
        Configuration conf = job.getConf();
        com.chinamobile.bcbsp.io.InputFormat<?, ?> input = ReflectionUtils
                .newInstance(job.getInputFormatClass(), conf);
        input.initialize(job.getConf());
        List<org.apache.hadoop.mapreduce.InputSplit> splits = input
                .getSplits(job);

        int maxSplits = job.getNumPartition();
        int splitNum = splits.size();
        double factor = splitNum / ( float ) maxSplits;
        if (factor > 1.0) {
            job.setInt(Constants.USER_BC_BSP_JOB_SPLIT_FACTOR,
                    ( int ) Math.ceil(factor));
            LOG.info("[Split Adjust Factor] " + ( int ) Math.ceil(factor));
            LOG.info("[Partition Num] " + maxSplits);
            splits = input.getSplits(job);
            splitNum = splits.size();
        }

        T[] array = ( T[] ) splits
                .toArray(new org.apache.hadoop.mapreduce.InputSplit[splits
                        .size()]);

        // sort the splits into order based on size, so that the biggest
        // go first
        Arrays.sort(array, new NewSplitComparator());
        DataOutputStream out = writeSplitsFileHeader(conf, submitSplitFile,
                array.length);
        try {
            if (array.length != 0) {
                DataOutputBuffer buffer = new DataOutputBuffer();
                RawSplit rawSplit = new RawSplit();
                SerializationFactory factory = new SerializationFactory(conf);
                Serializer<T> serializer = factory
                        .getSerializer(( Class<T> ) array[0].getClass());
                serializer.open(buffer);
                for (T split : array) {
                    rawSplit.setClassName(split.getClass().getName());
                    buffer.reset();
                    serializer.serialize(split);
                    rawSplit.setDataLength(split.getLength());
                    rawSplit.setBytes(buffer.getData(), 0, buffer.getLength());
                    rawSplit.setLocations(split.getLocations());
                    rawSplit.write(out);
                }
                serializer.close();
            }
        } finally {
            out.close();
        }
        return splitNum;
    }

    private static final int CURRENT_SPLIT_FILE_VERSION = 0;
    private static final byte[] SPLIT_FILE_HEADER = "SPL".getBytes();

    private DataOutputStream writeSplitsFileHeader(Configuration conf,
            Path filename, int length) throws IOException {
        // write the splits to a file for the job tracker
        FileSystem fs = filename.getFileSystem(conf);
        FSDataOutputStream out = FileSystem.create(fs, filename,
                new FsPermission(JOB_FILE_PERMISSION));
        out.write(SPLIT_FILE_HEADER);
        WritableUtils.writeVInt(out, CURRENT_SPLIT_FILE_VERSION);
        WritableUtils.writeVInt(out, length);
        return out;
    }

    /**
     * Read a splits file into a list of raw splits
     * 
     * @param in
     *            the stream to read from
     * @return the complete list of splits
     * @throws IOException
     *             NEU change in version-0.2.3 add new function: 直接从Hadoop中引用过来的
     */
    public static RawSplit[] readSplitFile(DataInput in) throws IOException {
        byte[] header = new byte[SPLIT_FILE_HEADER.length];
        in.readFully(header);
        if (!Arrays.equals(SPLIT_FILE_HEADER, header)) {
            throw new IOException("Invalid header on split file");
        }
        int vers = WritableUtils.readVInt(in);
        if (vers != CURRENT_SPLIT_FILE_VERSION) {
            throw new IOException("Unsupported split version " + vers);
        }
        int len = WritableUtils.readVInt(in);
        RawSplit[] result = new RawSplit[len];
        for (int i = 0; i < len; ++i) {
            result[i] = new RawSplit();
            result[i].readFields(in);
        }
        return result;
    }

    /**
     * Monitor a job and print status in real-time as progress is made and tasks
     * fail.
     * 
     * @param job
     * @param info
     * @return true, if job is successful
     * @throws IOException
     * @throws InterruptedException
     */
    public boolean monitorAndPrintJob(BSPJob job, RunningJob info)
            throws IOException, InterruptedException {
        String lastReport = null;
        LOG.info("Running job : " + info.getID());
        StringBuffer sb = new StringBuffer("JOB FINISHED");
        sb.append("\n*************************************************************");
        long startTime = System.currentTimeMillis();

        try {
            while (!info.isComplete()) {
                Thread.sleep(3000);
                long step = info.progress();
                String report = "the current supersteps number : " + step;

                if (!report.equals(lastReport)) {
                    LOG.info(report);
                    lastReport = report;
                }
            }

            if (info.isSuccessful()) {
                sb.append("\n    INFO       : The job is finished successfully");
            }
            if (info.isKilled()) {
                sb.append("\n    WARN       : The job is killed by user");
            }

            double totalTime = (System.currentTimeMillis() - startTime) / 1000.0;
            sb.append("\n    STATISTICS : Total supersteps   : "
                    + info.progress());
            sb.append("\n                 Total time(seconds): " + totalTime);
            sb.append("\n*************************************************************");
            LOG.info(sb.toString());
            return job.isSuccessful();
        } catch (Exception e) {
            sb.append("\n    ERROR      : " + e.getMessage());
            sb.append("\n    ERROR      : The job is viewed as killed by system");
            double totalTime = (System.currentTimeMillis() - startTime) / 1000.0;
            sb.append("\n    STATISTICS : Total supersteps    : " + lastReport);
            sb.append("\n                Total time(seconds) : " + totalTime);
            sb.append("\n*************************************************************");
            LOG.info(sb.toString());

            return false;
        }
    }

    /**
     * Grab the controller system directory path where job-specific files are to
     * be placed.
     * 
     * @return the system directory where job-specific files are to be placed.
     */
    public Path getSystemDir() {
        if (sysDir == null) {
            sysDir = new Path(jobSubmitClient.getSystemDir());
        }
        return sysDir;
    }

    public static void runJob(BSPJob job) throws FileNotFoundException,
            ClassNotFoundException, InterruptedException, IOException {
        BSPJobClient jc = new BSPJobClient(job.getConf());

        RunningJob running = jc.submitJobInternal(job);
        BSPJobID jobId = running.getID();
        LOG.info("Running job: " + jobId.toString());

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }

            if (running.isComplete()) {
                break;
            }

            running = jc.getJob(jobId);
        }

        LOG.info("Job complete: " + jobId);
        LOG.info("The total number of supersteps: " + running.getSuperstepCount());
        jc.close();
    }

    /**
     * Get an RunningJob object to track an ongoing job. Returns null if the id
     * does not correspond to any known job.
     * 
     * @throws IOException
     */
    private RunningJob getJob(BSPJobID jobId) throws IOException {
        JobStatus status = jobSubmitClient.getJobStatus(jobId);
        if (status != null) {
            return new NetworkedJob(status);
        } else {
            return null;
        }
    }

    /**
     * Get status information about the BSP cluster
     * 
     * @param detailed
     *            if true then get a detailed status including the groomserver
     *            names
     * 
     * @return the status information about the BSP cluster as an object of
     *         {@link ClusterStatus}.
     * 
     * @throws IOException
     */
    public ClusterStatus getClusterStatus(boolean detailed) throws IOException {
        return jobSubmitClient.getClusterStatus(detailed);
    }

    @SuppressWarnings("deprecation")
    @Override
    public int run(String[] args) throws Exception {
        int exitCode = -1;
        if (args.length < 1) {
            displayUsage("");
            return exitCode;
        }

        // process arguments
        String cmd = args[0];
        boolean listJobs = false;
        boolean listAllJobs = false;
        boolean listActiveWorkerManagers = false;
        boolean killJob = false;
        boolean submitJob = false;
        boolean getStatus = false;
        boolean listJobTasks = false;
        boolean listBspController = false;
        boolean setCheckPoint = false;
        String submitJobFile = null;
        String jobid = null;
        String checkpointCmd = null;

        BSPConfiguration conf = new BSPConfiguration(getConf());
        init(conf);

        if ("-list".equals(cmd)) {
            if (args.length != 1
                    && !(args.length == 2 && "all".equals(args[1]))) {
                displayUsage(cmd);
                return exitCode;
            }
            if (args.length == 2 && "all".equals(args[1])) {
                listAllJobs = true;
            } else {
                listJobs = true;
            }
        } else if ("-workers".equals(cmd)) {
            if (args.length != 1) {
                displayUsage(cmd);
                return exitCode;
            }
            listActiveWorkerManagers = true;
        } else if ("-submit".equals(cmd)) {
            if (args.length == 1) {
                displayUsage(cmd);
                return exitCode;
            }

            submitJob = true;
            submitJobFile = args[1];
        } else if ("-kill".equals(cmd)) {
            if (args.length != 2) {
                displayUsage(cmd);
                return exitCode;
            }
            killJob = true;
            jobid = args[1];

        } else if ("-status".equals(cmd)) {
            if (args.length != 2) {
                displayUsage(cmd);
                return exitCode;
            }
            jobid = args[1];
            getStatus = true;

            // TODO Later, below functions should be implemented
            // with the Fault Tolerant mechanism.
        } else if ("-list-staffs".equals(cmd)) {
            if (args.length != 2) {
                displayUsage(cmd);
                return exitCode;
            }
            jobid = args[1];
            listJobTasks = true;

        } else if ("-setcheckpoint".equals(cmd)) {
            if (args.length != 3) {
                displayUsage(cmd);
                return exitCode;
            }
            jobid = args[1];
            checkpointCmd = args[2];
            setCheckPoint = true;

        } else if ("-master".equals(cmd)) {
            if (args.length != 1) {
                displayUsage(cmd);
                return exitCode;
            }
            listBspController = true;

        } else if ("-kill-staff".equals(cmd)) {
            System.out.println("This function is not implemented yet.");
            return exitCode;
        } else if ("-fail-staff".equals(cmd)) {
            System.out.println("This function is not implemented yet.");
            return exitCode;
        }

        BSPJobClient jc = new BSPJobClient(new BSPConfiguration());
        if (listJobs) {
            listJobs();
            exitCode = 0;
        } else if (listAllJobs) {
            listAllJobs();
            exitCode = 0;
        } else if (listActiveWorkerManagers) {
            listActiveWorkerManagers();
            exitCode = 0;
        } else if (submitJob) {
            BSPConfiguration tConf = new BSPConfiguration(new Path(
                    submitJobFile));
            RunningJob job = jc.submitJob(new BSPJob(tConf));
            System.out.println("Created job " + job.getID().toString());
        } else if (killJob) {
            RunningJob job = jc.getJob(new BSPJobID().forName(jobid));
            if (job == null) {
                System.out.println("Could not find job " + jobid);
            } else {
                job.killJob();
                System.out.println("Killed job " + jobid);
            }
            exitCode = 0;
        } else if (getStatus) {
            RunningJob job = jc.getJob(new BSPJobID().forName(jobid));
            if (job == null) {
                System.out.println("Could not find job " + jobid);
            } else {
                JobStatus jobStatus = jobSubmitClient.getJobStatus(job.getID());
                String start = "NONE", finish = "NONE";
                if (jobStatus.getStartTime() != 0) {
                    start = new Date(jobStatus.getStartTime()).toLocaleString();
                }
                if (jobStatus.getFinishTime() != 0) {
                    finish = new Date(jobStatus.getFinishTime()).toLocaleString();
                }
                System.out.printf("States are:\n\tRunning : 1\tSucceded : 2"
                        + "\tFailed : 3\tPrep : 4\n");
                System.out.printf("Job name: %s\tUserName: %s\n", job.getJobName(), jobStatus.getUsername());
                System.out.printf("ID: %s\tState: %d\tSuperStep: %d\tStartTime: %s\tEndTime: %s\n", jobStatus.getJobID(),
                        jobStatus.getRunState(), jobStatus.progress(), start, finish);

                exitCode = 0;
            }
        } else if (listJobTasks) {
            StaffAttemptID id[] = jobSubmitClient.getStaffStatus(new BSPJobID()
                    .forName(jobid));
            for (StaffAttemptID ids : id) {
                System.out.println(ids);
            }
            StaffStatus ss[] = jobSubmitClient.getStaffDetail(new BSPJobID()
                    .forName(jobid));
            System.out.println("array list size is" + ss.length);
        } else if (setCheckPoint) {
            if (checkpointCmd.equals("next")) {
                jobSubmitClient.setCheckFrequencyNext(new BSPJobID().forName(jobid));
            } else {
                jobSubmitClient.setCheckFrequency(new BSPJobID().forName(jobid), Integer.valueOf(checkpointCmd));
            }
        } else if (listBspController) {
            listBspController();
            exitCode = 0;
        }

        return 0;
    }

    private void listBspController() throws IOException {
        ClusterStatus c = jobSubmitClient.getClusterStatus(true);
        System.out.println("Controller:" + BSPController.getAddress(getConf()));
        System.out.println("Controller state is :" + c.getBSPControllerState());
    }

    /**
     * Display usage of the command-line tool and terminate execution
     */
    private void displayUsage(String cmd) {
        String prefix = "Usage: bcbsp job ";
        String taskStates = "running, completed";
        if ("-submit".equals(cmd)) {
            System.err.println(prefix + "[" + cmd + " <job-file>]");
        } else if ("-status".equals(cmd) || "-kill".equals(cmd)) {
            System.err.println(prefix + "[" + cmd + " <job-id>]");
        } else if ("-list".equals(cmd)) {
            System.err.println(prefix + "[" + cmd + " [all]]");
        } else if ("-kill-staff".equals(cmd) || "-fail-staff".equals(cmd)) {
            System.err.println(prefix + "[" + cmd + " <staff-id>]");
        } else if ("-list-active-workermanagers".equals(cmd)) {
            System.err.println(prefix + "[" + cmd + "]");
        } else if ("-list-staffs".equals(cmd)) {
            System.err.println(prefix + "[" + cmd + " <job-id> <staff-state>]. "
                    + "Valid values for <staff-state> are " + taskStates);
        } else {
            System.err.printf(prefix + "<command> <args>\n");
            System.err.printf("\t[-submit <job-file>]\n");
            System.err.printf("\t[-status <job-id>]\n");
            System.err.printf("\t[-kill <job-id>]\n");
            System.err.printf("\t[-list [all]]\n");
            System.err.printf("\t[-list-active-workermanagers]\n");
            System.err.println("\t[-list-attempt <job-id> " + "<staff-state>]\n");
            System.err.printf("\t[-kill-staff <staff-id>]\n");
            System.err.printf("\t[-fail-staff <staff-id>]\n\n");
        }
    }

    /**
     * Dump a list of currently running jobs
     * 
     * @throws IOException
     */
    private void listJobs() throws IOException {
        JobStatus[] jobs = jobsToComplete();
        if (jobs == null)
            jobs = new JobStatus[0];

        System.out.printf("%d jobs currently running\n", jobs.length);
        displayJobList(jobs);
    }

    /**
     * Dump a list of all jobs submitted.
     * 
     * @throws IOException
     */
    private void listAllJobs() throws IOException {
        JobStatus[] jobs = getAllJobs();
        if (jobs == null)
            jobs = new JobStatus[0];
        System.out.printf("%d jobs submitted\n", jobs.length);
        System.out.printf("States are:\n\tRunning : 1\tSucceded : 2"
                + "\tFailed : 3\tPrep : 4\n");
        displayJobList(jobs);
    }

   public void displayJobList(JobStatus[] jobs) {
        System.out.printf("JobId\tState\tStartTime\tUserName\n");
        for (JobStatus job : jobs) {
            System.out.printf("%s\t%d\t%d\t%s\n", job.getJobID(),
                    job.getRunState(), job.getStartTime(), job.getUsername());
        }
    }

    /**
     * Display the list of active worker servers
     */
    private void listActiveWorkerManagers() throws IOException {
        ClusterStatus c = jobSubmitClient.getClusterStatus(true);
        int runningClusterStaffs = c.getRunningClusterStaffs();
        String[] activeWorkerManagersName = c.getActiveWorkerManagersName();
        System.out
                .println("running ClusterStaffs is : " + runningClusterStaffs);
        for (String workerManagerName : activeWorkerManagersName) {
            System.out.println(workerManagerName + "      active");
        }
    }

    /**
   */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new BSPJobClient(), args);
        System.exit(res);
    }
}
