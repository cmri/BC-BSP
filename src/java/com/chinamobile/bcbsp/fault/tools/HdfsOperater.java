/**
 * CopyRight by Chinamobile
 * 
 * HdfsOperater.java
 */
package com.chinamobile.bcbsp.fault.tools;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class HdfsOperater {
    
    public static final Log LOG = LogFactory.getLog(HdfsOperater.class);

    public static String uploadHdfs(String localPath, String destPath) {
        InputStream in = null;
        OutputStream out = null;
        try {
            String localSrc = localPath;
            File srcFile = new File(localSrc);
            if (srcFile.exists()) {
                // String dst = hostName + dirPath;
                in = new BufferedInputStream(new FileInputStream(localSrc));
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(URI.create(destPath), conf);
                out = fs.create(new Path(destPath),

                new Progressable() {
                    public void progress() {
                        
                    }
                });
                IOUtils.copyBytes(in, out, 4096, true);

                out.flush();
                out.close();
                in.close();
                return destPath;
            } else {
                return "error";
            }
        } catch (FileNotFoundException e) {
            LOG.error("[uploadHdfs]", e);
            return "error";
        } catch (IOException e) {
            LOG.error("[uploadHdfs]", e);
            try {
                if (out != null)
                    out.flush();
                if (out != null)
                    out.close();
                if (in != null)
                    in.close();
            } catch (IOException e1) {
                LOG.error("[uploadHdfs]", e1);
            }
            return "error";
        }

    }

    public static void downloadHdfs(String srcfilePath, String destFilePath) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(srcfilePath), conf);
            FSDataInputStream hdfsInStream = fs.open(new Path(srcfilePath));
            File dstFile = new File(destFilePath);
            if (!dstFile.getParentFile().exists()) {
                dstFile.getParentFile().mkdirs();
            }
            OutputStream out = new FileOutputStream(destFilePath);
            byte[] ioBuffer = new byte[1024];
            int readLen = hdfsInStream.read(ioBuffer);

            while (-1 != readLen) {
                out.write(ioBuffer, 0, readLen);
                readLen = hdfsInStream.read(ioBuffer);
            }
            out.close();
            hdfsInStream.close();
            fs.close();
        } catch (FileNotFoundException e) {
            LOG.error("[downloadHdfs]", e);
        } catch (IOException e) {
            LOG.error("[downloadHdfs]", e);
        }
    }

    public static void deleteHdfs(String hdfsFile) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);
            fs.deleteOnExit(new Path(hdfsFile));
            fs.close();
        } catch (IOException e) {
            LOG.error("[deleteHdfs]", e);
        }
    }

    // check the existence of file on hdfs
    public static boolean isHdfsFileExist(String hdfsFilePath) {
        Configuration conf = new Configuration();
        Path path = new Path(hdfsFilePath);
        try {
            FileSystem fs = FileSystem.get(URI.create(hdfsFilePath), conf);
            if (fs.exists(path) == true)
                return true;
            else
                return false;
        } catch (IOException e) {
            LOG.error("[isHdfsFileExist]", e);
            return false;
        }
    }

    public static boolean isHdfsDirEmpty(String hdfsFilePath) {

        try {
            Configuration conf = new Configuration();
            Path path = new Path(hdfsFilePath);

            FileSystem fs = FileSystem.get(URI.create(hdfsFilePath), conf);
            FileStatus status[] = fs.globStatus(path);

            if (status == null || status.length == 0) {

                throw new FileNotFoundException("Cannot access " + hdfsFilePath
                        + ": No such file or directory.");

            }
            for (int i = 0; i < status.length; i++) {
                long totalSize = fs.getContentSummary(status[i].getPath())
                        .getLength();
                @SuppressWarnings("unused")
                String pathStr = status[i].getPath().toString();
                return totalSize == 0 ? true : false;
            }

        } catch (IOException e) {
            LOG.error("[isHdfsDirEmpty]", e);
            return false;
        }
        return false;

    }
}