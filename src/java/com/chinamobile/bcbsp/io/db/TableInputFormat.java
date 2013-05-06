/**
 * CopyRight by Chinamobile
 * 
 * TableInputFormat.java 
 */
package com.chinamobile.bcbsp.io.db;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

/**
 * Convert HBase tabular data into a format that is consumable by BCBSP job.
 */
public class TableInputFormat extends TableInputFormatBase {

    private final Log LOG = LogFactory.getLog(TableInputFormat.class);

    /** Job parameter that specifies the input table. */
    public static final String INPUT_TABLE = "hbase.mapreduce.inputtable";
    /**
     * Base-64 encoded scanner. All other SCAN_ confs are ignored if this is
     * specified. See {@link TableBCBSPJobUtil#convertScanToString(Scan)} for
     * more details.
     */
    public static final String SCAN = "hbase.mapreduce.scan";
    /** Column Family to Scan */
    public static final String SCAN_COLUMN_FAMILY = "hbase.mapreduce.scan.column.family";
    /** Space delimited list of columns to scan. */
    public static final String SCAN_COLUMNS = "hbase.mapreduce.scan.columns";
    /** The timestamp used to filter columns with a specific timestamp. */
    public static final String SCAN_TIMESTAMP = "hbase.mapreduce.scan.timestamp";
    /**
     * The starting timestamp used to filter columns with a specific range of
     * versions.
     */
    public static final String SCAN_TIMERANGE_START = "hbase.mapreduce.scan.timerange.start";
    /**
     * The ending timestamp used to filter columns with a specific range of
     * versions.
     */
    public static final String SCAN_TIMERANGE_END = "hbase.mapreduce.scan.timerange.end";
    /** The maximum number of version to return. */
    public static final String SCAN_MAXVERSIONS = "hbase.mapreduce.scan.maxversions";
    /** Set to false to disable server-side caching of blocks for this scan. */
    public static final String SCAN_CACHEBLOCKS = "hbase.mapreduce.scan.cacheblocks";
    /** The number of rows for caching that will be passed to scanners. */
    public static final String SCAN_CACHEDROWS = "hbase.mapreduce.scan.cachedrows";

    @SuppressWarnings("deprecation")
    @Override
    public void initialize(Configuration configuration) {
        this.conf = HBaseConfiguration.create(configuration);
        this.conf.set("hbase.master", configuration.get("hbase.master"));
        this.conf.set("hbase.zookeeper.quorum",
                configuration.get("hbase.zookeeper.quorum"));
        this.conf.set(SCAN, configuration.get(SCAN));
        this.conf.set(INPUT_TABLE, configuration.get(INPUT_TABLE));
        this.conf
                .set(SCAN_COLUMN_FAMILY, configuration.get(SCAN_COLUMN_FAMILY));

        String tableName = conf.get(INPUT_TABLE);
        try {
            setHTable(new HTable(conf, tableName));
        } catch (Exception e) {
            LOG.error(StringUtils.stringifyException(e));
        }

        Scan scan = null;

        if (conf.get(SCAN) != null) {
            try {
                scan = TableBCBSPJobUtil.convertStringToScan(conf.get(SCAN));
            } catch (IOException e) {
                LOG.error("An error occurred.", e);
            }
        } else {
            try {
                scan = new Scan();

                if (conf.get(SCAN_COLUMNS) != null) {
                    scan.addColumns(conf.get(SCAN_COLUMNS));
                }

                if (conf.get(SCAN_COLUMN_FAMILY) != null) {
                    scan.addFamily(Bytes.toBytes(conf.get(SCAN_COLUMN_FAMILY)));
                }

                if (conf.get(SCAN_TIMESTAMP) != null) {
                    scan.setTimeStamp(Long.parseLong(conf.get(SCAN_TIMESTAMP)));
                }

                if (conf.get(SCAN_TIMERANGE_START) != null
                        && conf.get(SCAN_TIMERANGE_END) != null) {
                    scan.setTimeRange(
                            Long.parseLong(conf.get(SCAN_TIMERANGE_START)),
                            Long.parseLong(conf.get(SCAN_TIMERANGE_END)));
                }

                if (conf.get(SCAN_MAXVERSIONS) != null) {
                    scan.setMaxVersions(Integer.parseInt(conf
                            .get(SCAN_MAXVERSIONS)));
                }

                if (conf.get(SCAN_CACHEDROWS) != null) {
                    scan.setCaching(Integer.parseInt(conf.get(SCAN_CACHEDROWS)));
                }

                // false by default, full table scans generate too much BC churn
                scan.setCacheBlocks((conf.getBoolean(SCAN_CACHEBLOCKS, false)));
            } catch (Exception e) {
                LOG.error(StringUtils.stringifyException(e));
            }
        }

        setScan(scan);
    }
}