/**
 * CopyRight by Chinamobile
 * 
 * Constants.java
 */
package com.chinamobile.bcbsp;

/**
 * Constants
 * 
 * This class maintains all relative global variables.
 * 
 * @author
 * @version
 */
public interface Constants {
    
    // /////////////////////////////////////
    // Constants for BC-BSP ZooKeeper
    // /////////////////////////////////////

    public static final String ZOOKEEPER_QUORUM = "bcbsp.zookeeper.quorum";
    public static final String ZOOKEEPER_CONFIG_NAME = "zoo.cfg";
    public static final String ZOOKEPER_CLIENT_PORT = "bcbsp.zookeeper.clientPort";
    public static final int DEFAULT_ZOOKEPER_CLIENT_PORT = 2181;
    public static final String BSPJOB_ZOOKEEPER_DIR_ROOT = "/bspRoot";
    public static final String COMMAND_NAME = "command";

    // /////////////////////////////////////
    // Constants for BC-BSP Framework
    // /////////////////////////////////////

    public static final String BC_BSP_CONTROLLER_ADDRESS = "bcbsp.controller.address";
    public static final String BC_BSP_WORKERMANAGER_RPC_HOST = "bcbsp.workermanager.rpc.hostname";
    public static final String DEFAULT_BC_BSP_WORKERMANAGER_RPC_HOST = "0.0.0.0";
    public static final String BC_BSP_WORKERMANAGER_RPC_PORT = "bcbsp.workermanager.rpc.port";
    public static final String BC_BSP_WORKERMANAGER_REPORT_ADDRESS = "bcbsp.workermanager.report.address";
    public static final String BC_BSP_WORKERAGENT_HOST = "bcbsp.workeragent.host";
    public static final String DEFAULT_BC_BSP_WORKERAGENT_HOST = "0.0.0.0";
    public static final String BC_BSP_WORKERAGENT_PORT = "bcbsp.workeragent.port";
    public static final int DEFAULT_BC_BSP_WORKERAGENT_PORT = 61000;
    public static final String BC_BSP_WORKERMANAGER_MAXSTAFFS = "bcbsp.workermanager.staff.max";

    public static final String BC_BSP_SHARE_DIRECTORY = "bcbsp.share.dir";
    public static final String BC_BSP_CHECKPOINT_DIRECTORY = "bcbsp.checkpoint.dir";
    public static final String BC_BSP_LOCAL_DIRECTORY = "bcbsp.local.dir";
    public static final String BC_BSP_LOCAL_SUBDIR_CONTROLLER = "controller";
    public static final String BC_BSP_LOCAL_SUBDIR_WORKERMANAGER = "workerManager";

    public static final String HEART_BEAT_INTERVAL = "bcbsp.heartbeat.interval";
    public static final String HEART_BEAT_TIMEOUT = "bcbsp.heartbeat.timeout";
    public static final String SLEEP_TIMEOUT = "bcbsp.worker.sleep.timeout";
    public static final String BC_BSP_FAILED_JOB_PER_WORKER = "bcbsp.max.faied.job.worker";
    
    public static final String BC_BSP_JVM_VERSION = "bcbsp.jvm.bits";
    public static final String BC_BSP_HDFS_NAME = "fs.default.name";

    // /////////////////////////////////////
    // Constants for BC-BSP Job
    // /////////////////////////////////////

    public static final String DEFAULT_BC_BSP_JOB_CHECKPOINT_FREQUENCY = "bcbsp.checkpoint.frequency";
    public static final String BC_BSP_JOB_RECOVERY_ATTEMPT_MAX = "bcbsp.recovery.attempt.max";
    public static final String BC_BSP_STAFF_RECOVERY_ATTEMPT_MAX = "bcbsp.staff.recovery.attempt.max";
    public static final String BC_BSP_CHECKPOINT_WRITEPATH = "bcbsp.checkpoint.dir";
    public static final String BC_BSP_RECOVERY_READPATH = "bcbsp.checkpoint.dir";
    public static final String BC_BSP_MEMORY_DATA_PERCENT = "bcbsp.memory.data.percent";

    public static final String USER_BC_BSP_JOB_WRITEPARTITION_CLASS = "job.writepartition.class";
    public static final String USER_BC_BSP_JOB_PARTITIONER_CLASS = "job.partitioner.class";
    public static final String USER_BC_BSP_JOB_RECORDPARSE_CLASS="job.recordparse.class";
    public static final String USER_BC_BSP_JOB_ISDIVIDE="job.isdivide";
    public static final String USER_BC_BSP_JOB_SENDTHREADNUMBER="job.partition.sendthreadnumber";
    public static final int    USER_BC_BSP_JOB_SENDTHREADNUMBER_DEFAULT=10;
    public static final String USER_BC_BSP_JOB_TOTALCACHE_SIZE="job.writepartition.totalcache.size";
    public static final int    USER_BC_BSP_JOB_TOTALCACHE_SIZE_DEFAULT=10;
    public static final String USER_BC_BSP_JOB_BALANCE_FACTOR="job.writepartition.balance.factor";
    public static final float  USER_BC_BSP_JOB_BALANCE_FACTOR_DEFAULT=0.01f;
    public static final String USER_BC_BSP_JOB_JAR = "job.jar";
    public static final String USER_BC_BSP_JOB_NAME = "job.name";
    public static final String USER_BC_BSP_JOB_USER_NAME = "job.user.name";
    public static final String USER_BC_BSP_JOB_WORK_CLASS = "job.work.class";
    public static final String USER_BC_BSP_JOB_INPUT_FORMAT_CLASS = "job.inputformat.class";
    public static final String USER_BC_BSP_JOB_SPLIT_SIZE = "job.input.split.size";
    public static final String USER_BC_BSP_JOB_OUTPUT_FORMAT_CLASS = "job.outputformat.class";
    public static final String USER_BC_BSP_JOB_AGGREGATE_NAMES = "job.aggregate.names";
    public static final String USER_BC_BSP_JOB_AGGREGATE_NUM = "job.aggregate.num";
    public static final String USER_BC_BSP_JOB_WORKING_DIR = "job.working.dir";
    public static final String USER_BC_BSP_JOB_CHECKPOINT_FREQUENCY = "job.user.checkpoint.frequency";
    public static final String USER_BC_BSP_JOB_SUPERSTEP_MAX = "job.user.superstep.max";
    public static final String USER_BC_BSP_JOB_STAFF_NUM = "job.staff.num";
    public static final String USER_BC_BSP_JOB_PRIORITY = "job.priority";
    public static final String USER_BC_BSP_JOB_PARTITION_TYPE = "job.partition.type";
    public static final String USER_BC_BSP_JOB_PARTITION_NUM = "job.partition.num";
    public static final String USER_BC_BSP_JOB_INPUT_DIR = "job.input.dir";
    public static final String USER_BC_BSP_JOB_OUTPUT_DIR = "job.output.dir";
    public static final String USER_BC_BSP_JOB_SPLIT_FILE = "job.split.file";
    public static final String USER_BC_BSP_JOB_SPLIT_FACTOR = "job.split.factor";
    public static final String USER_BC_BSP_JOB_COMBINER_CLASS = "job.combiner.class";
    public static final String USER_BC_BSP_JOB_GRAPHDATA_CLASS = "job.graphdata.class";
    public static final String USER_BC_BSP_JOB_COMBINER_DEFINE_FLAG = "job.combiner.define.flag";
    public static final String USER_BC_BSP_JOB_COMBINER_RECEIVE_FLAG = "job.combiner.receive.flag";
    public static final String USER_BC_BSP_JOB_SEND_THRESHOLD = "job.send.threshold";
    public static final String USER_BC_BSP_JOB_SEND_COMBINE_THRESHOLD = "job.send.combine.threshold";
    public static final String USER_BC_BSP_JOB_RECEIVE_COMBINE_THRESHOLD = "job.receive.combine.threshold";
    public static final String USER_BC_BSP_JOB_MESSAGE_PACK_SIZE = "job.message.pack.size";
    public static final String USER_BC_BSP_JOB_MAX_PRODUCER_NUM = "job.max.producer.number";
    public static final String USER_BC_BSP_JOB_MAX_CONSUMER_NUM = "job.max.consumer.number";
    public static final String USER_BC_BSP_JOB_MEMORY_DATA_PERCENT = "job.memory.data.percent";
    public static final String USER_BC_BSP_JOB_MEMORY_BETA = "job.memory.beta";
    public static final String USER_BC_BSP_JOB_MEMORY_HASHBUCKET_NO = "job.memory.hashbucket.number";
    public static final String USER_BC_BSP_JOB_GRAPH_DATA_VERSION = "job.graph.data.version";
    public static final String USER_BC_BSP_JOB_MESSAGE_QUEUES_VERSION = "job.message.queues.version";
    
    public static final String USER_BC_BSP_JOB_VERTEX_CLASS = "job.vertex.class";
    public static final String USER_BC_BSP_JOB_EDGE_CLASS = "job.edge.class";

    // /////////////////////////////////////
    // Constants for BC-BSP Utility
    // ////////////////////////////////////

    public static final String UTF8_ENCODING = "UTF-8";
    public static final String SPLIT_FLAG = ":";
    public static final String KV_SPLIT_FLAG = "\t";
    public static final String SPACE_SPLIT_FLAG = " ";
    public static final String SSC_SPLIT_FLAG = "#";
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    // /////////////////////////////////////
    // Constants for BC-BSP Disk Cache
    // ////////////////////////////////////
    
    public static final int GRAPH_BITMAP_BUCKET_NUM_BYTES = 320;
    public static final String GRAPH_BUCKET_FILE_HEADER = "[Graph data hash bucket]";
    public static final String MSG_BUCKET_FILE_HEADER = "[Message data hash bucket]";
    public static final String MSG_QUEUE_FILE_HEADER = "[Message data queue]";
    
    // /////////////////////////////////////
    // Complicated Constants
    // ////////////////////////////////////
    
    public static class PRIORITY {
        public static final String LOWER = "5";
        public static final String LOW = "4";
        public static final String NORMAL = "3";
        public static final String HIGH = "2";
        public static final String HIGHER = "1";
    }

    public static class PARTITION_TYPE {
        public static final String HASH = "hash";
        public static final String RANGE = "range";
    }

    public static class COMMAND_TYPE {
        public static final int START = 1;
        public static final int STOP = 2;
        public static final int START_AND_CHECKPOINT = 3;
        public static final int START_AND_RECOVERY = 4;
    }

    public static class SUPERSTEP_STAGE {
        public static final int SCHEDULE_STAGE = 1;
        public static final int LOAD_DATA_STAGE = 2;
        public static final int FIRST_STAGE = 3;
        public static final int SECOND_STAGE = 4;
        public static final int WRITE_CHECKPOINT_SATGE = 5;
        public static final int READ_CHECKPOINT_STAGE = 6;
        public static final int SAVE_RESULT_STAGE = 7;
    }

    public static class SATAFF_STATUS {
        public static final int RUNNING = 1;
        public static final int FAULT = 2;
        public static final int SUCCEED = 3;
    }
}
