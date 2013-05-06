/**
 * CopyRight by Chinamobile
 * 
 * DirRecord.java
 */
package com.chinamobile.bcbsp.fault.storage;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.mortbay.log.Log;

/**
 * record the filePath in local and hdfs 
 *
 */
@SuppressWarnings("serial")
public class DirRecord implements Serializable,Cloneable {

    private int hdfsFileNum = 13;
    private int localFileNum = 5;

    private int indexl = 0;
    private int indexh = 0;

    private String[] hdfsfilelists = new String[hdfsFileNum];
    private File[] localfilelists = new File[localFileNum];

    boolean copyFlag =false;
    // ---------------------localfile=--------------------------------------
/**
 * record file path in DirRecord
 * return last month directory position in order to compress
 */
    public int pushLocalFile(File file) {
        localfilelists[indexl] = file;
        indexl = (indexl + 1) % localFileNum;
        deleteLocalFile(indexl);
        return (((indexl - 2) + localFileNum) % localFileNum);
    }

    public File getLocalFile(int indexl) {

        return localfilelists[indexl];
    } 

    public void deleteLocalFile(int indexl) {
        if(localfilelists[indexl] == null){
            return;
        }
        else if(localfilelists[indexl].exists()) {
            try {
                File localParentFile = localfilelists[indexl].getParentFile();
                del(localfilelists[indexl].getAbsolutePath());
                Log.info("localParentFile == null"+(localParentFile==null));
                if (localParentFile.listFiles().length == 0) {//delete year
                    del(localParentFile.getAbsolutePath());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            localfilelists[indexl] = null;
        }

    }

    // --------------HDFS--------------------------------

    /**
     * return overdue directory and delete it from hdfs
     */
    public int pushHdfsFile(String filepath) {
        hdfsfilelists[indexh] = filepath;
        indexh = (indexh + 1) % hdfsFileNum;
        return indexh;
    }

    public String getHdfsFile(int indexh) {
        return hdfsfilelists[indexh];
    }

    public void deleteHdfsFile(int indexh) {

        if (hdfsfilelists[indexh] != null) {
            hdfsfilelists[indexh] = null;
        }

    }

    public int getIndexl() {
        return indexl;
    }

    public void setIndexl(int indexl) {
        this.indexl = indexl;
    }

    public int getIndexh() {
        return indexh;
    }

    public void setIndexh(int indexh) {
        this.indexh = indexh;
    }

    public int getLocalFileNum() {
        return localFileNum;
    }

    public void setLocalFileNum(int localFileNum) {
        this.localFileNum = localFileNum;
    }

    public int getHdfsFileNum() {
        return hdfsFileNum;
    }

    public void setHdfsFileNum(int hdfsFileNum) {
        this.hdfsFileNum = hdfsFileNum;
    }

    public void del(String filepath) throws IOException {
        File f = new File(filepath);
        if (f.exists() && f.isDirectory()) {
            if (f.listFiles().length == 0) {
                f.delete();
            } else {
                File delFile[] = f.listFiles();
                int i = f.listFiles().length;
                for (int j = 0; j < i; j++) {
                    if (delFile[j].isDirectory()) {
                        del(delFile[j].getAbsolutePath());
                    }
                    delFile[j].delete();
                }
            }
            f.delete();
        }
    }
    
    public DirRecord clone() throws CloneNotSupportedException{
        
        DirRecord dr ;
        dr = (DirRecord)super.clone();
        dr.hdfsfilelists = this.hdfsfilelists.clone();
        dr.localfilelists = this.localfilelists.clone();
        return dr;
    }

    public boolean isCopyFlag() {
        return copyFlag;
    }

    public void setCopyFlag(boolean copyFlag) {
        this.copyFlag = copyFlag;
    }
}
