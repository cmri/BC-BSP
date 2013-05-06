/**
 * CopyRight by Chinamobile
 * 
 * GraphDataForDisk.java
 */
package com.chinamobile.bcbsp.graph;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.ObjectSizer;

/**
 * GraphDataForDisk Graph data manager for disk supported.
 * 
 * @author
 * @version
 */
public class GraphDataForDisk implements GraphDataInterface {

    // For Log
    private static final Log LOG = LogFactory.getLog(GraphDataForDisk.class);
    // For time accumulation
    private long writeDiskTime = 0;
    /** Clock */
    private long readDiskTime = 0;
    /** Clock */

    private Class<? extends Vertex<?, ?, ?>> vertexClass;
    private Class<? extends Edge<?, ?>> edgeClass;

    /** The metadata for a bucket */
    class BucketMeta {
        // Have been accessed by which number of super step
        public int superStepCount;
        // Is on disk flag
        public boolean onDiskFlag;
        // The length of the bucket by Bytes.
        public long length;
        // The length of the part of the bucket still in memory by Bytes.
        public long lengthInMemory;
        // Number of all nodes in the bucket.
        public int count;
        // Number of active nodes in the bucket
        public int activeCount;
    }

    /**
     * The beta parameter for the proportion of data memory for the graph data,
     * 1-beta for messages data
     */
    private float beta;

    /**
     * The parameter for the percentage of the heap memory for the data memory
     * (graph & messages)
     */
    private float dataPercent;

    /** Hash bucket number */
    private int hashBucketNumber;

    /** Hash buckets of the headnode list */
    @SuppressWarnings("unchecked")
    private ArrayList<ArrayList<Vertex>> hashBuckets;

    /**
     * Bitmaps for each hash bucket, the bits is sorted(from right) the same as
     * the list of nodes in each bucket. A int(4 bytes) represents 32 head
     * nodes.
     */
    private ArrayList<ArrayList<Integer>> bitmaps;

    /** Meta data table for hash buckets */
    private ArrayList<BucketMeta> metaTable;

    private final ObjectSizer sizer; // Object sizer.

    private int sizeOfVertex; // Size of Vertex type instance.(Bytes)
    private int sizeOfEdge; // Size of Edge type instance.(Bytes)
    private long totalSizeOfVertex; // Total size of Vertex.(Bytes)
    private int totalCountOfVertex; // Total count of Vertex.
    @SuppressWarnings("unused")
    private long totalSizeOfEdge; // Total size of Edge.(Bytes)
    private int totalCountOfEdge; // Total count of Edge.

    private final int sizeOfRef; // Size of a reference.
    private final int sizeOfInteger; // Size of an Integer.

    private long sizeOfGraphSpace; // The total space for graph data.(Bytes)
    private long sizeThreshold; // The threshold size for graph data.(Bytes)

    private long sizeOfGraphDataInMem; // The current size of graph data.(Bytes)
    private long sizeOfBitmapsInMem; // The size of bitmaps in memory.(Bytes)
    private long sizeOfMetaTable; // The size of metaTable in memory.(Bytes)

    private BSPJobID jobID;
    private int partitionID;

    private File fileRoot;
    private File graphDataFile;
    private File graphDataFile_bucket;
    private FileReader fr_graphData;
    private BufferedReader br_graphData;
    private FileWriter fw_graphData;
    private BufferedWriter bw_graphData;

    private int sizeForAll; // Total number of head nodes.

    private int[] sortedBucketIndexList;

    private int currentBucketIndex; // Current pointer of bucket index for
                                    // traversal.
    private int currentNodeIndex; // Current pointer of node index in bucket for
                                  // traversal.

    private Staff staff;
    
    public GraphDataForDisk(){
        //initialize some const variables
        BSPConfiguration conf = new BSPConfiguration();
        if (conf.getInt(Constants.BC_BSP_JVM_VERSION, 32) == 64) {
            sizer = ObjectSizer.forSun64BitsVM();
        } else {
            sizer = ObjectSizer.forSun32BitsVM();
        }
        this.sizeOfRef = sizer.sizeOfRef();
        this.sizeOfInteger = sizer.sizeOf(new Integer(0));
    }

    public void setStaff(Staff staff) {
        this.staff = staff;
    }

    public void initialize() {
        BSPJob job = this.staff.getConf();
        int partitionID = this.staff.getPartition();
        initialize(job, partitionID);
    }

    /**
     * 
     * 
     * @param job
     * @param partitionID
     */
    @SuppressWarnings("unchecked")
    public void initialize(BSPJob job, int partitionID) {

        vertexClass = job.getVertexClass();
        edgeClass = job.getEdgeClass();

        LOG.info("========== Initializing Graph Data For Disk ==========");

        this.dataPercent = job.getMemoryDataPercent(); // Default 0.8

        this.jobID = job.getJobID();
        this.partitionID = partitionID;
        this.beta = job.getBeta();
        this.hashBucketNumber = job.getHashBucketNumber();

        LOG.info("[beta] = " + this.beta);
        LOG.info("[hashBucketNumber] = " + this.hashBucketNumber);

        this.hashBuckets = new ArrayList<ArrayList<Vertex>>(hashBucketNumber);

        // So the bitmaps's length decides the maximum nodes of a bucket is
        // 320*32.
        this.bitmaps = new ArrayList<ArrayList<Integer>>(hashBucketNumber);

        this.metaTable = new ArrayList<BucketMeta>(hashBucketNumber);

        // Initialize the meta table and bitmaps.
        for (int i = 0; i < hashBucketNumber; i++) {

            this.hashBuckets.add(null);

            // init the meta table.
            BucketMeta meta = new BucketMeta();
            meta.superStepCount = -1;
            meta.onDiskFlag = false;
            meta.length = 0;
            meta.lengthInMemory = 0;
            meta.count = 0;
            meta.activeCount = 0;
            metaTable.add(meta);

            // init the bitmapsCache.
            ArrayList<Integer> bitmap = new ArrayList<Integer>(
                    Constants.GRAPH_BITMAP_BUCKET_NUM_BYTES);
            for (int j = 0; j < Constants.GRAPH_BITMAP_BUCKET_NUM_BYTES; j++)
                bitmap.add(0);
            this.bitmaps.add(bitmap);
        }

        // Initialize the size of objects and data structures.
        int sizeOfMetaBucket = sizer.sizeOf(new BucketMeta());
        this.sizeOfMetaTable = (sizeOfMetaBucket + sizeOfRef)
                * hashBucketNumber;

        int sizeOfBitmap = sizer.sizeOf(new ArrayList<Integer>());
        this.sizeOfBitmapsInMem = (sizeOfBitmap + sizeOfRef) * hashBucketNumber;

        Vertex<?, ?, ?> tmpVertex = null;
        Edge<?, ?> tmpEdge = null;
        try {
            tmpVertex = this.vertexClass.newInstance();
            tmpEdge = this.edgeClass.newInstance();
        } catch (InstantiationException e) {
            LOG.error("[GraphDataForDisk] caught: ", e);
        } catch (IllegalAccessException e) {
            LOG.error("[GraphDataForDisk] caught: ", e);
        }

        this.sizeOfVertex = sizer.sizeOf(tmpVertex);
        this.sizeOfEdge = sizer.sizeOf(tmpEdge);

        LOG.info("[Default initial size of Vertex] = " + this.sizeOfVertex
                + "B");
        LOG.info("[Default initial size of Edge] = " + this.sizeOfEdge + "B");

        // Get the memory mxBean.
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        // Get the heap memory usage.
        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
        long maxHeapSize = memoryUsage.getMax();

        LOG.info("[JVM max Heap size] = " + maxHeapSize / 1048576 + "MB");

        this.sizeOfGraphSpace = ( long ) (maxHeapSize * dataPercent * beta);
        this.sizeThreshold = ( long ) (sizeOfGraphSpace);

        this.sizeOfGraphDataInMem = 0;
        this.sizeForAll = 0;
        this.totalSizeOfVertex = 0;
        this.totalCountOfVertex = 0;
        this.totalSizeOfEdge = 0;
        this.totalCountOfEdge = 0;

        this.sortedBucketIndexList = new int[hashBucketNumber];

        this.fileRoot = new File("/tmp/bcbsp/" + this.jobID.toString() + "/"
                + "partition-" + this.partitionID);
        this.graphDataFile = new File(this.fileRoot + "/" + "GraphData");

        LOG.info("[size of Graph Data Space] = " + this.sizeOfGraphSpace
                / 1048576 + "MB");
        LOG.info("[threshold of Graph Data] = " + this.sizeThreshold / 1048576
                + "MB");
        LOG.info("======================================================");
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized void addForAll(Vertex vertex) {

        this.sizeForAll++;

        String vertexID = String.valueOf(vertex.getVertexID());
        int hashCode = vertexID.hashCode();
        int hashIndex = hashCode % this.hashBucketNumber; // bucket index
        hashIndex = (hashIndex < 0 ? hashIndex + this.hashBucketNumber
                : hashIndex);

        /** Add the vertex to the right hash bucket. */
        ArrayList<Vertex> hashBucket = this.hashBuckets.get(hashIndex);
        // When the hash bucket reference is null, create a bucket.
        if (hashBucket == null) {
            hashBucket = new ArrayList<Vertex>();
        }
        hashBucket.add(vertex);
        this.hashBuckets.set(hashIndex, hashBucket);

        // Evaluate the memory size of the new Vertex.
        int newVertexSize = this.sizeOfRef + this.sizer.sizeOf(vertex);
        this.totalSizeOfVertex += newVertexSize;
        this.totalCountOfVertex++;
        this.totalCountOfEdge += vertex.getEdgesNum();
        /** Add the vertex's size to the length of the bucket's meta. */
        BucketMeta meta = this.metaTable.get(hashIndex);
        meta.length = meta.length + newVertexSize;
        meta.lengthInMemory = meta.lengthInMemory + newVertexSize;
        meta.count = meta.count + 1;
        meta.activeCount = meta.activeCount + 1;
        this.metaTable.set(hashIndex, meta);

        /** Add the headnode's size to the graph data's size. */
        this.sizeOfGraphDataInMem = this.sizeOfGraphDataInMem + newVertexSize;

        /** Set the bitmap's refered bit to 1 */
        setBitmapTrue(hashIndex, meta.activeCount - 1, this.bitmaps);

        // Add a new Integer into the bitmaps every 8 nodes added.
        if (this.sizeForAll % 32 == 1) {
            this.sizeOfBitmapsInMem = this.sizeOfBitmapsInMem
                    + this.sizeOfInteger + this.sizeOfRef;
        }

        onVertexAdded();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Vertex get(int index) {

        // When it's the first time to access head node,
        // sort the buckets into an order.
        if (index == 0) {
            sortBucketsForAll();
        }

        // Locate the bucket for the index.
        int activeCount = 0;
        int i = 0;
        while ((activeCount = this.metaTable.get(this.sortedBucketIndexList[i]).activeCount) < index + 1) {
            index = index - activeCount;
            i++;
        }
        int bucketIndex = this.sortedBucketIndexList[i];

        // Locate the nodeIndex in bucket for the index.
        int nodeIndex = 0; // prictical node index.
        int counter = 0; // active counter.
        while (counter < index + 1) {
            // If the bit for the nodeIndex is active
            if (getBitmap(bucketIndex, nodeIndex, this.bitmaps)) {
                counter++;
            }
            nodeIndex++;
        }
        nodeIndex = nodeIndex - 1;

        // If the bucket is on disk.
        if (this.metaTable.get(bucketIndex).onDiskFlag) {
            long bucketLength = this.metaTable.get(bucketIndex).length;
            long idleLength = 0; // Idle space that can be swap out.
            // Look up the bucket before the i bucket for in-memory bucket's
            // space.
            // Swap the in-memory buckets out on disk until the idleLength
            // enough
            // for the on-disk bucket to swap in.
            for (int j = 0; j < i; j++) {
                int m = this.sortedBucketIndexList[j];
                // If the m bucket is in memory, swap it out,
                // and add the length to the idleLength.
                if (!this.metaTable.get(m).onDiskFlag) {
                    idleLength = idleLength + this.metaTable.get(m).length;
                    try {
                        saveBucket(m);
                    } catch (IOException e) {
                        LOG.error("[GraphDataForDisk] caught: ", e);
                    }
                }
                // When idle is enough, break the for.
                if (idleLength >= bucketLength) {
                    break;
                }
            }// end-for

            /** Load bucket */
            try {
                loadBucket(bucketIndex);
            } catch (IOException e) {
                LOG.error("[GraphDataForDisk] caught: ", e);
            }
        }

        // For set method use.
        this.currentBucketIndex = bucketIndex;
        this.currentNodeIndex = nodeIndex;

        return this.hashBuckets.get(bucketIndex).get(nodeIndex);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Vertex getForAll(int index) {

        // When it's the first time to access head node,
        // sort the buckets into an order.
        if (index == 0) {
            sortBucketsForAll();
        }

        // Locate the bucket for the index.
        int count = 0;
        int i = 0;
        while ((count = this.metaTable.get(this.sortedBucketIndexList[i]).count) < index + 1) {
            index = index - count;
            // Set the accessed bucket's referrence to null.
            // this.hashBuckets.set(this.sortedBucketIndexList[i], null);
            i++;
        }
        int bucketIndex = this.sortedBucketIndexList[i];

        // If the bucket is on disk.
        if (this.metaTable.get(bucketIndex).onDiskFlag) {
            long bucketLength = this.metaTable.get(bucketIndex).length;
            long idleLength = this.sizeThreshold - this.sizeOfGraphDataInMem; 
            // Idle space that can be swap out.
            // Look up the bucket before the i bucket for in-memory bucket's space.
            // Swap the in-memory buckets out on disk until the idleLength
            // enough for the on-disk bucket to swap in.
            for (int j = 0; j < i; j++) {

                // When idle is enough, break the for.
                if (idleLength >= bucketLength) {
                    break;
                }

                int m = this.sortedBucketIndexList[j];
                // If the m bucket is in memory, swap it out,
                // and add the length to the idleLength.
                if (!this.metaTable.get(m).onDiskFlag) {
                    idleLength = idleLength + this.metaTable.get(m).length;
                    try {
                        saveBucket(m);
                    } catch (IOException e) {
                        LOG.error("[GraphDataForDisk] caught: ", e);
                    }
                }

            }// end-for

            /** Load bucket */
            try {
                loadBucket(bucketIndex);
            } catch (IOException e) {
                LOG.error("[GraphDataForDisk] caught: ", e);
            }
        }

        // For set method use.
        this.currentBucketIndex = bucketIndex;
        this.currentNodeIndex = index;

        return this.hashBuckets.get(bucketIndex).get(index);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void set(int index, Vertex vertex, boolean activeState) {

        this.hashBuckets.get(this.currentBucketIndex).set(
                this.currentNodeIndex, vertex);

        boolean oldActiveState = this.getBitmap(this.currentBucketIndex,
                this.currentNodeIndex, this.bitmaps);

        if (activeState) {
            if (!oldActiveState) {
                this.setBitmapTrue(this.currentBucketIndex,
                        this.currentNodeIndex, this.bitmaps);
                this.metaTable.get(this.currentBucketIndex).activeCount++;
            }
        } else {
            if (oldActiveState) {
                this.setBitmapFalse(this.currentBucketIndex,
                        this.currentNodeIndex, this.bitmaps);
                this.metaTable.get(this.currentBucketIndex).activeCount--;
            }
        }
    }

    @Override
    public int size() {

        int count = 0;
        for (BucketMeta meta : this.metaTable) {
            count = count + meta.activeCount;
        }
        return count;
    }

    @Override
    public int sizeForAll() {

        return this.sizeForAll;
    }

    private void onVertexAdded() {

        // When the graph data's size exceeds the threshold.
        if (this.sizeOfGraphDataInMem + this.sizeOfBitmapsInMem >= this.sizeThreshold) {

            // LOG.info("[size fo Graph Data In Memory] = " +
            // this.sizeOfGraphDataInMem/1024 + "KB");

            // If the root dir does not exit, create it.
            if (!this.fileRoot.exists()) {
                this.fileRoot.mkdirs();
            }
            // If the graph data dir does not exit, create it.
            if (!this.graphDataFile.exists()) {
                this.graphDataFile.mkdir();
            }

            int bucketIndex = findLongestBucket();

            try {
                saveBucket(bucketIndex);
            } catch (IOException e) {
                LOG.error("[GraphDataForDisk] caught: ", e);
            }
        }
    }

    /**
     * Find the longest bucket now in the memory.
     * 
     * @return bucketIndex
     */
    private int findLongestBucket() {

        int bucketIndex = 0;
        long longestLength = 0;
        for (int i = 0; i < this.hashBucketNumber; i++) {
            BucketMeta meta = metaTable.get(i);
            // Find the longest but lengthInMemory is not zero.
            if (meta.length > longestLength && meta.lengthInMemory != 0) {
                longestLength = meta.length;
                bucketIndex = i;
            }
        }
        return bucketIndex;
    }

    /**
     * Save the given bucket to the disk file.
     * 
     * @param index
     *            of the bucket
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private void saveBucket(int index) throws IOException {

        long start = System.currentTimeMillis();
        /** Clock */

        this.graphDataFile_bucket = new File(this.graphDataFile + "/"
                + "bucket-" + index);

        boolean isNewFile = false;
        // The bucket file does not exit, create it.
        if (!this.graphDataFile_bucket.exists()) {
            this.graphDataFile_bucket.createNewFile();
            isNewFile = true;
        }

        // Append to the bucket file by line.
        this.fw_graphData = new FileWriter(graphDataFile_bucket, true);
        this.bw_graphData = new BufferedWriter(fw_graphData, 65536);

        if (isNewFile) {
            // Write the file header.
            this.bw_graphData.write(Constants.GRAPH_BUCKET_FILE_HEADER + "-"
                    + index);
        }

        ArrayList<Vertex> hashBucket = this.hashBuckets.get(index);
        for (int i = 0; i < hashBucket.size(); i++) {
            this.bw_graphData.newLine();
            this.bw_graphData.write(hashBucket.get(i).intoString());
        }
        this.bw_graphData.close();
        this.fw_graphData.close();

        // Update the meta data for the bucket.
        BucketMeta meta = metaTable.get(index);
        // Update the size fo graph data.
        this.sizeOfGraphDataInMem = this.sizeOfGraphDataInMem
                - meta.lengthInMemory;
        meta.onDiskFlag = true; // Set the on disk flag true.
        meta.lengthInMemory = 0; // Set the length in memory to 0.
        metaTable.set(index, meta);

        // Set the bucket's reference in the list to null.
        this.hashBuckets.set(index, null);

        this.writeDiskTime = this.writeDiskTime
                + (System.currentTimeMillis() - start);
        /** Clock */
    }

    /**
     * Load the given bucket from disk file into the memory array list.
     * 
     * @param index
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private void loadBucket(int index) throws IOException {

        long start = System.currentTimeMillis();
        /** Clock */

        this.graphDataFile_bucket = new File(this.graphDataFile + "/"
                + "bucket-" + index);

        if (!this.graphDataFile_bucket.exists()) {
            throw new IOException("Bucket file does not exit!");
        }
        // Open file readers.
        this.fr_graphData = new FileReader(this.graphDataFile_bucket);
        this.br_graphData = new BufferedReader(this.fr_graphData);

        // Read the file header.
        @SuppressWarnings("unused")
        String bucketHeader = this.br_graphData.readLine();

        ArrayList<Vertex> hashBucket = this.hashBuckets.get(index);
        if (hashBucket == null) {
            hashBucket = new ArrayList<Vertex>();
        }
        String vertexData;
        try {
            while ((vertexData = this.br_graphData.readLine()) != null) {
                Vertex vertex = this.vertexClass.newInstance();
                vertex.fromString(vertexData);
                hashBucket.add(vertex);
            }
        } catch (Exception e) {
            LOG.error("[GraphDataForDisk] caught: ", e);
        }
        this.br_graphData.close();
        this.fr_graphData.close();

        // Update the meta data for the bucket.
        BucketMeta meta = metaTable.get(index);
        // Update the size of graph data.
        this.sizeOfGraphDataInMem = this.sizeOfGraphDataInMem
                + (meta.length - meta.lengthInMemory);
        meta.onDiskFlag = false;
        meta.lengthInMemory = meta.length;
        metaTable.set(index, meta);

        this.hashBuckets.set(index, hashBucket);

        if (!this.graphDataFile_bucket.delete()) {
            throw new IOException("Bucket file delete failed!");
        }

        this.readDiskTime = this.readDiskTime
                + (System.currentTimeMillis() - start);
        /** Clock */
    }

    /**
     * Set the bitmap's bit for the hashIndex hash bucket's nodeIndex headnode
     * to be 1.
     * 
     * @param hashIndex
     * @param nodeIndex
     * @param aBitmaps
     */
    private void setBitmapTrue(int hashIndex, int nodeIndex,
            ArrayList<ArrayList<Integer>> aBitmaps) {
        // Get the refered bitmap of the bucket.
        ArrayList<Integer> bitmap = aBitmaps.get(hashIndex);
        // The node bit belongs to the point int element of the array.
        int point = ( int ) (nodeIndex / 32);// a int has 32 bits.
        // If the nodeIndex over the bitmap's size, append new 32 bit.
        if ((point + 1) > bitmap.size()) {
            bitmap.add(new Integer(0));
        }
        // The bit shift number for the int element.
        int shift = 31 - nodeIndex % 32;
        // The unit for the int element to or.
        int orUnit = 1;
        orUnit = orUnit << shift;
        // Or the int element with the unit to set the bit to 1.
        bitmap.set(point, bitmap.get(point) | orUnit);
        // Set back the bitmap into the bucket.
        aBitmaps.set(hashIndex, bitmap);
    }

    /**
     * Set the bitmap's bit for the hashIndex hash bucket's nodeIndex headnode
     * to be 0.
     * 
     * @param hashIndex
     * @param nodeIndex
     * @param aBitmaps
     */
    private void setBitmapFalse(int hashIndex, int nodeIndex,
            ArrayList<ArrayList<Integer>> aBitmaps) {
        // Get the refered bitmap of the bucket.
        ArrayList<Integer> bitmap = aBitmaps.get(hashIndex);
        // The node bit belongs to the point int element of the array.
        int point = ( int ) (nodeIndex / 32);// a int has 32 bits.
        // If the nodeIndex over the bitmap's size, append new 32 bit.
        if ((point + 1) > bitmap.size()) {
            bitmap.add(new Integer(0));
        }
        // The bit shift number for the int element.
        int shift = 31 - nodeIndex % 32;
        // The unit for the int element to and.
        int andUnit = 1;
        andUnit = andUnit << shift;
        andUnit = ~andUnit;
        // And the int element with the unit to set the bit to 0.
        bitmap.set(point, bitmap.get(point) & andUnit);
        // Set back the bitmap into the bucket.
        aBitmaps.set(hashIndex, bitmap);
    }

    /**
     * Get the bitmap's bit for the hashIndex hash bucket's nodeIndex
     * headnode.If it's 1, returns true, otherwise, returns false.
     * 
     * @param hashIndex
     * @param nodeIndex
     * @param aBitmaps
     * @return
     */
    private boolean getBitmap(int hashIndex, int nodeIndex,
            ArrayList<ArrayList<Integer>> aBitmaps) {
        // Get the refered bitmap of the bucket.
        ArrayList<Integer> bitmap = aBitmaps.get(hashIndex);
        // The node bit belongs to the point int element of the array.
        int point = ( int ) (nodeIndex / 32);// a int has 32 bits.
        // The bit shift number for the int element.
        int shift = 31 - nodeIndex % 32;
        // The unit for the int element to and.
        int andUnit = 1;
        andUnit = andUnit << shift;
        // And the int element with the unit to get the referred bit.
        int result = bitmap.get(point) & andUnit;

        if (result == 0) {
            return false;
        } else
            return true;
    }

    /**
     * Sort the buckets for traversing all the nodes. After this operation, the
     * buckets are in a order as in-memory buckets descending by length at
     * first, and on-disk buckets ascending by length at last.
     */
    private void sortBucketsForAll() {

        int begin = 0;
        int end = this.hashBucketNumber - 1;

        // 1st loop, sink all the on disk buckets to end.
        for (int i = 0; i < this.hashBucketNumber; i++) {
            BucketMeta meta = this.metaTable.get(i);
            if (meta.onDiskFlag) { // The on disk bucket should sink.
                this.sortedBucketIndexList[end] = i;
                end--;
            } else {
                this.sortedBucketIndexList[begin] = i;
                begin++;
            }
        }
        // come here, end points to the end of buckets in memory;
        // begin points to the start of the buckets on disk.

        // 2nd loop, bubble sort the in memory buckets by length descending.
        for (int i = 0; i < end; i++) {
            for (int j = i + 1; j <= end; j++) {
                if (this.metaTable.get(this.sortedBucketIndexList[i]).length < this.metaTable
                        .get(this.sortedBucketIndexList[j]).length) {
                    // Swap
                    int temp = this.sortedBucketIndexList[i];
                    this.sortedBucketIndexList[i] = this.sortedBucketIndexList[j];
                    this.sortedBucketIndexList[j] = temp;
                }
            }
        }

        // 3rd loop, bubble sort the on disk buckets by length ascending.
        for (int i = begin; i < this.hashBucketNumber - 1; i++) {
            for (int j = i + 1; j <= this.hashBucketNumber - 1; j++) {
                if (this.metaTable.get(this.sortedBucketIndexList[i]).length > this.metaTable
                        .get(this.sortedBucketIndexList[j]).length) {
                    // Swap
                    int temp = this.sortedBucketIndexList[i];
                    this.sortedBucketIndexList[i] = this.sortedBucketIndexList[j];
                    this.sortedBucketIndexList[j] = temp;
                }
            }
        }
    }// end-sortBucketsForAll

    @SuppressWarnings("unchecked")
    @Override
    public void clean() {

        for (int i = 0; i < this.hashBucketNumber; i++) {
            this.graphDataFile_bucket = new File(this.graphDataFile + "/"
                    + "bucket-" + i);
            if (this.graphDataFile_bucket.exists()) {
                if (!this.graphDataFile_bucket.delete()) {
                    LOG.warn("[File] Delete file:" + this.graphDataFile_bucket + " failed!");
                }
            }
        }

        if (this.graphDataFile.exists()) {
            if (!this.graphDataFile.delete()) {
                LOG.warn("[File] Delete directory:" + this.graphDataFile + " failed!");
            }
        }

        this.hashBuckets = new ArrayList<ArrayList<Vertex>>(hashBucketNumber);

        // So the bitmaps's length decides the maximum nodes of a bucket is
        // 320*32.
        this.bitmaps = new ArrayList<ArrayList<Integer>>(hashBucketNumber);
        this.metaTable = new ArrayList<BucketMeta>(hashBucketNumber);

        // Initialize the meta table and bitmaps.
        for (int i = 0; i < hashBucketNumber; i++) {

            this.hashBuckets.add(null);

            // init the meta table.
            BucketMeta meta = new BucketMeta();
            meta.superStepCount = -1;
            meta.onDiskFlag = false;
            meta.length = 0;
            meta.lengthInMemory = 0;
            meta.count = 0;
            meta.activeCount = 0;
            // meta.activeCountCache = 0;
            metaTable.add(meta);

            // init the bitmapsCache.
            ArrayList<Integer> bitmap = new ArrayList<Integer>(
                    Constants.GRAPH_BITMAP_BUCKET_NUM_BYTES);
            for (int j = 0; j < Constants.GRAPH_BITMAP_BUCKET_NUM_BYTES; j++)
                bitmap.add(0);
            this.bitmaps.add(bitmap);
        }

        int sizeOfMetaBucket = sizer.sizeOf(new BucketMeta());
        this.sizeOfMetaTable = (sizeOfMetaBucket + sizeOfRef)
                * hashBucketNumber;

        int sizeOfBitmap = sizer.sizeOf(new ArrayList<Integer>());
        this.sizeOfBitmapsInMem = (sizeOfBitmap + sizeOfRef) * hashBucketNumber;

        Vertex<?, ?, ?> tmpVertex = null;
        Edge<?, ?> tmpEdge = null;
        try {
            tmpVertex = this.vertexClass.newInstance();
            tmpEdge = this.edgeClass.newInstance();
        } catch (InstantiationException e) {
            LOG.error("[GraphDataForDisk] caught: ", e);
        } catch (IllegalAccessException e) {
            LOG.error("[GraphDataForDisk] caught: ", e);
        }

        this.sizeOfVertex = sizer.sizeOf(tmpVertex);
        this.sizeOfEdge = sizer.sizeOf(tmpEdge);

        // Get the memory mxBean.
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        // Get the heap memory usage.
        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
        long maxHeapSize = memoryUsage.getMax();

        this.sizeOfGraphSpace = ( long ) (maxHeapSize * dataPercent * beta);
        this.sizeThreshold = ( long ) (sizeOfGraphSpace);

        this.sizeOfGraphDataInMem = 0;
        this.sizeForAll = 0;
        this.totalSizeOfVertex = 0;
        this.totalCountOfVertex = 0;
        this.totalSizeOfEdge = 0;
        this.totalCountOfEdge = 0;

        this.sortedBucketIndexList = new int[hashBucketNumber];

        this.fileRoot = new File("/tmp/bcbsp/" + this.jobID.toString() + "/"
                + "partition-" + this.partitionID);
        this.graphDataFile = new File(this.fileRoot + "/" + "GraphData");
    }

    @Override
    public void finishAdd() {

        if (this.totalCountOfVertex > 0)
            this.sizeOfVertex = ( int ) (this.totalSizeOfVertex / this.totalCountOfVertex);
        LOG.info("[Graph Data For Disk] Finish loading data!!!");
        showMemoryInfo();

        this.sizeThreshold = this.sizeThreshold - this.sizeOfBitmapsInMem;

        // Sort the buckets for all in memory buckets on the left
        // descending by length, and the buckets with part on disk
        // on the right ascending by length.
        sortBucketsForAll();

        // Traverse from right to left, and try best to load buckets
        // that part on disk all into memory, if too big for memory
        // to contain, then write it the whole on to disk.
        for (int i = this.hashBucketNumber - 1; i >= 0; i--) {

            int bucketIndex = this.sortedBucketIndexList[i];
            BucketMeta meta = this.metaTable.get(bucketIndex);

            // If the bucket has part on disk and part in memory.
            if (meta.lengthInMemory > 0 && meta.lengthInMemory < meta.length) {

                long sizeOnDisk = meta.length - meta.lengthInMemory;

                // The memory still has enough space for the size on disk.
                if (sizeOnDisk < (this.sizeThreshold - this.sizeOfGraphDataInMem)) {
                    try {
                        loadBucket(bucketIndex);
                    } catch (IOException e) {
                        LOG.error("[GraphDataForDisk] caught: ", e);
                    }
                } else { // The memory cannot contain the size on disk.
                    try {
                        saveBucket(bucketIndex);
                    } catch (IOException e) {
                        LOG.error("[GraphDataForDisk] caught: ", e);
                    }
                }
            }
        }// end-for

        this.showSortedHashBucketsInfo();
    }

    @Override
    public long getActiveCounter() {

        int count = 0;
        for (BucketMeta meta : this.metaTable) {
            count = count + meta.activeCount;
        }
        return count;
    }

    @Override
    public boolean getActiveFlagForAll(int index) {
        return getBitmap(this.currentBucketIndex, this.currentNodeIndex,
                this.bitmaps);
    }

    public void showMemoryInfo() {
        LOG.info("----------------- Memory Info of Graph -----------------");
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
        long used = memoryUsage.getUsed();
        long committed = memoryUsage.getCommitted();
        LOG.info("<Real> [Memory used] = " + used / 1048576 + "MB");
        LOG.info("<Real> [Memory committed] = " + committed / 1048576 + "MB");
        LOG.info("<Evaluate> [size of Vertex] = " + this.sizeOfVertex + "B");
        LOG.info("<Evaluate> [total size of Vertex] = "
                + this.totalSizeOfVertex / 1048576 + "MB");
        LOG.info("<Evaluate> [total count of Vertex] = "
                + this.totalCountOfVertex);
        LOG.info("<Evaluate> [total count of Edge] = " + this.totalCountOfEdge);
        LOG.info("<Evaluate> [size fo MetaTable In Memory] = "
                + this.sizeOfMetaTable / 1024 + "KB");
        LOG.info("<Evaluate> [size of Graph Data In Memory] = "
                + this.sizeOfGraphDataInMem / 1048576 + "MB");
        LOG.info("<Evaluate> [size fo Bitmaps In Memory] = "
                + this.sizeOfBitmapsInMem / 1024 + "KB");
        LOG.info("<Evaluate> [size of Graph Data Threshold] = "
                + this.sizeThreshold / 1048576 + "MB");
        LOG.info("----------------- -------------------- -----------------");
        this.showHashBucketsInfo();
        LOG.info("[==>Clock<==] <GraphDataForDisk: save bucket> totally used "
                + this.writeDiskTime / 1000f + " seconds");
        /** Clock */
        LOG.info("[==>Clock<==] <GraphDataForDisk: load bucket> totally used "
                + this.readDiskTime / 1000f + " seconds");
        /** Clock */
        LOG.info("[==>Clock<==] <GraphDataForDisk: Disk I/O> totally used "
                + (this.writeDiskTime + this.readDiskTime) / 1000f + " seconds");
        /** Clock */
        this.writeDiskTime = 0;
        /** Clock */
        this.readDiskTime = 0;
        /** Clock */
    }

    private void showHashBucketsInfo() {
        LOG.info("------------ Buckets Info of Graph ------------");

        long maxLength = 0;
        for (int i = 0; i < this.metaTable.size(); i++) {
            BucketMeta meta = this.metaTable.get(i);
            if (meta.length > maxLength) {
                maxLength = meta.length;
            }
        }

        for (int i = 0; i < this.metaTable.size(); i++) {
            BucketMeta meta = this.metaTable.get(i);
            String out = "[Bucket-" + i + "] ";
            if (meta.onDiskFlag) {
                out = out + "OnDisk ";
            } else {
                out = out + "       ";
            }

            out = out + meta.lengthInMemory / 1048576 + "MB - " + meta.length
                    / 1048576 + "MB ";

            int nMax = 30;
            int nAll = ( int ) (nMax * (( float ) meta.length / ( float ) maxLength));
            int nMem = ( int ) (nAll * (( float ) meta.lengthInMemory / ( float ) meta.length));
            int nDisk = nAll - nMem;
            for (int j = 0; j < nMem; j++) {
                out = out + "-";
            }
            for (int j = 0; j < nDisk; j++) {
                out = out + "*";
            }
            LOG.info(out);
        }

        LOG.info("------------ --------------------- ------------");
    }

    private void showSortedHashBucketsInfo() {
        LOG.info("------------ Buckets Info of Graph ------------");

        long maxLength = 0;
        for (int i = 0; i < this.metaTable.size(); i++) {
            BucketMeta meta = this.metaTable.get(i);
            if (meta.length > maxLength) {
                maxLength = meta.length;
            }
        }

        for (int i = 0; i < this.sortedBucketIndexList.length; i++) {
            int p = this.sortedBucketIndexList[i];
            BucketMeta meta = this.metaTable.get(p);
            String out = "[Bucket-" + p + "] ";
            if (meta.onDiskFlag) {
                out = out + "OnDisk ";
            } else {
                out = out + "       ";
            }

            out = out + meta.lengthInMemory / 1048576 + "MB - " + meta.length
                    / 1048576 + "MB ";

            int nMax = 30;
            int nAll = ( int ) (nMax * (( float ) meta.length / ( float ) maxLength));
            int nMem = ( int ) (nAll * (( float ) meta.lengthInMemory / ( float ) meta.length));
            int nDisk = nAll - nMem;
            for (int j = 0; j < nMem; j++) {
                out = out + "-";
            }
            for (int j = 0; j < nDisk; j++) {
                out = out + "*";
            }
            LOG.info(out);
        }

        LOG.info("------------ --------------------- ------------");
    }

    @Override
    public int getEdgeSize() {
        return this.totalCountOfEdge;
    }
}
