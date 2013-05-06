/**
 * CopyRight by Chinamobile
 * 
 * GetRecord.java
 */
package com.chinamobile.bcbsp.fault.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.chinamobile.bcbsp.fault.storage.DirRecord;
import com.chinamobile.bcbsp.fault.storage.ManageFaultLog;

public class GetRecord {

    private String hdfsRecordPath = null;
    private String localRecordPath = null;
    private ObjectInputStream ois = null;
    
    public static final Log LOG = LogFactory.getLog(GetRecord.class);

    public GetRecord(String localRecordPath, String hdfsRecordPath) {

        this.hdfsRecordPath = hdfsRecordPath;
        this.localRecordPath = localRecordPath;
    }

    public DirRecord getRecord() {
        DirRecord dr = null;
        if ((new File(localRecordPath)).exists()) {
            dr = getlocalRecord();
        } else {
            if (HdfsOperater.isHdfsFileExist(hdfsRecordPath)) {
                dr = gethdfsRecord();
                if (!checkUniformity(dr)) {
                    updateDirRecord(dr);
                }
            }
        }
        return dr;
    }

    private void updateDirRecord(DirRecord dr) {
      if(dr.getHdfsFile(((dr.getIndexh()-1)+dr.getHdfsFileNum())%dr.getHdfsFileNum())==null){  //至少有一个
          dr = null;
      }else{
          int hdfsIndex=(dr.getIndexh()-1)%dr.getHdfsFileNum();
          String localFile = dr.getHdfsFile(((dr.getIndexh()-1)+dr.getHdfsFileNum())%dr.getHdfsFileNum());
          String hdfsFile ;
          dr.setIndexl(0);
          for(int indexl=0;indexl<dr.getLocalFileNum();indexl++){
              dr.pushLocalFile(null);
          }
          for(int i =dr.getLocalFileNum()-2;i>0;i--){
              hdfsFile = dr.getHdfsFile(((hdfsIndex-i+1)+dr.getHdfsFileNum())%dr.getHdfsFileNum());
              if(hdfsFile!=null){
              localFile = downLoadToLocal(hdfsFile);
              dr.pushLocalFile(new File(localFile));
              }
              }
          String localDirPath = new File(localFile).getParentFile().getParentFile().getAbsolutePath()+File.separator+ManageFaultLog.getRecordFile();
          String hdfsNameNodeHostName = getHdfsNameNodeHostName();
          // add update the Dirrecord to disk
          dr.setCopyFlag(true);
          syschronizeDirRecordWithDisk(dr,localDirPath,hdfsNameNodeHostName);
          }
     }

    private String getHdfsNameNodeHostName() {
        Configuration conf = new Configuration(false);
        String HADOOP_HOME = System.getenv("HADOOP_HOME");
        String corexml = HADOOP_HOME + "/conf/core-site.xml";
        conf.addResource(new Path(corexml));
        String hdfsNamenodehostName = conf.get("fs.default.name");
        return hdfsNamenodehostName;
    }
    private void syschronizeDirRecordWithDisk(DirRecord dr,String localDirPath,String hdfsNameNodeHostName) {
       ObjectOutputStream oos=null;
        File recordfile = new File(localDirPath);
        try {
            if (!recordfile.getParentFile().exists()) {
                recordfile.getParentFile().mkdirs();
                               }
            oos = new ObjectOutputStream(new FileOutputStream(localDirPath));
            oos.writeObject(dr); // write the dr into recordPath;
            oos.close();
        } catch (FileNotFoundException e) {
            LOG.error("[syschronizeDirRecordWithDisk]", e);
        } catch (IOException e) {
            LOG.error("[syschronizeDirRecordWithDisk]", e);
        }
        HdfsOperater.uploadHdfs(recordfile.getAbsolutePath(), hdfsNameNodeHostName+recordfile.getAbsolutePath());
    }

    private String downLoadToLocal(String hdfsFile) {
        
       String LocalZipFile = getLocalFilePath(hdfsFile);
       HdfsOperater.downloadHdfs(hdfsFile, LocalZipFile);
       Zip.decompress(LocalZipFile);
       
       String localFile = LocalZipFile.substring(0,LocalZipFile.lastIndexOf("."));
       deleteFile(new File(LocalZipFile));
       return localFile;
    }
    public static  String getLocalFilePath(String hdfsFile){
        String s=hdfsFile;
        int pos;
        
        for(int i = 0;i<3;i++){
            pos = s.indexOf("/");
            s=s.substring(pos+1);
        }
        s="/"+s;
        return s;
    }

    private boolean checkUniformity(DirRecord dr) {
        int num = dr.getLocalFileNum();
        File localFile = null;
        for (int i = 0; i < num; i++) {
            localFile = dr.getLocalFile(i);
            if (localFile != null && !localFile.exists()) {
                return false;
            }
        }
        return true;
    }

    private void deleteFile(File file) {
        if (file.isFile() || file.listFiles().length == 0) {
            file.delete();
        } else {
            File[] files = file.listFiles();
            for (File f : files) {
                if (f.isFile()) {
                    f.delete();
                } else {
                    deleteFile(f);
                }
            }
            file.delete();
        }

    }

    private DirRecord gethdfsRecord() {

        HdfsOperater.downloadHdfs(hdfsRecordPath, localRecordPath);
        return getlocalRecord();
    }

    private DirRecord getlocalRecord() {

        DirRecord dr = null;
        try {
            ois = new ObjectInputStream(new FileInputStream(localRecordPath));

            dr = ( DirRecord ) ois.readObject();
            ois.close();
        } catch (FileNotFoundException e) {
            LOG.error("[getlocalRecord]", e);
        } catch (IOException e) {
            LOG.error("[getlocalRecord]", e);
        } catch (ClassNotFoundException e) {
            LOG.error("[getlocalRecord]", e);
        }
        return dr;
    }

}
