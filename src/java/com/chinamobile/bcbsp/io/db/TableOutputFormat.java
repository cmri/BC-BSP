/**
 * CopyRight by Chinamobile
 * 
 * TableOutputFormat.java
 */
package com.chinamobile.bcbsp.io.db;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;

public class TableOutputFormat<KEY> extends TableOutputFormatBase<KEY> {
    private final Log LOG = LogFactory.getLog(TableOutputFormat.class);

    /** Job parameter that specifies the output table. */
    public static final String OUTPUT_TABLE = "hbase.outputtable";
    /**
     * Optional job parameter to specify a peer cluster. Used specifying remote
     * cluster when copying between hbase clusters (the source is picked up from
     * <code>hbase-site.xml</code>).
     */
    public static final String QUORUM_ADDRESS = "hbase.mapred.output.quorum";

    /** Optional specification of the rs class name of the peer cluster */
    public static final String REGION_SERVER_CLASS = "hbase.mapred.output.rs.class";
    /** Optional specification of the rs impl name of the peer cluster */
    public static final String REGION_SERVER_IMPL = "hbase.mapred.output.rs.impl";

    /** The configuration. */
    private Configuration conf = null;

    public void initialize(Configuration otherConf) {
        this.conf = HBaseConfiguration.create(otherConf);
        this.conf.set("hbase.master", otherConf.get("hbase.master"));
        this.conf.set("hbase.zookeeper.quorum",
                otherConf.get("hbase.zookeeper.quorum"));
        String tableName = this.conf.get(OUTPUT_TABLE);

        String address = this.conf.get(QUORUM_ADDRESS);
        String serverClass = this.conf.get(REGION_SERVER_CLASS);
        String serverImpl = this.conf.get(REGION_SERVER_IMPL);
        try {
            if (address != null) {
                ZKUtil.applyClusterKeyToConf(this.conf, address);
            }
            if (serverClass != null) {
                this.conf.set(HConstants.REGION_SERVER_CLASS, serverClass);
                this.conf.set(HConstants.REGION_SERVER_IMPL, serverImpl);
            }
            HTable table = new HTable(this.conf, tableName);
            table.setAutoFlush(false);
            setTable(table);
            LOG.info("Created table instance for " + tableName);
        } catch (IOException e) {
            LOG.error("[initialize]", e);
        }
    }
}
