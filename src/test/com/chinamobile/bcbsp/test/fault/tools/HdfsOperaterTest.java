/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.fault.tools;

import java.io.File;

import junit.framework.TestCase;

import org.junit.Test;
import com.chinamobile.bcbsp.fault.tools.HdfsOperater;

public class HdfsOperaterTest extends TestCase {

    @Test
    public void testUploadHdfs() {
        String localPath = "test/com/chinamobile/bcbsp/test/fault/tools/upload";
        String destPath = "hdfsup";
        @SuppressWarnings("unused")
        String actual = HdfsOperater.uploadHdfs(localPath, destPath);
        assertTrue(HdfsOperater.isHdfsFileExist(destPath));
    }

    @Test
    public void testDownloadHdfs() {
        String srcfilePath = "hdfsup";
        String destFilePath = "test/com/chinamobile/bcbsp/test/fault/tools/download";
        HdfsOperater.downloadHdfs(srcfilePath, destFilePath);
        File file = new File(destFilePath);
        assertTrue(file.exists());
    }

    @Test
    public void testDeleteHdfs() {
        String hdfsFile = "delfile";
        HdfsOperater.deleteHdfs(hdfsFile);
        assertFalse(HdfsOperater.isHdfsFileExist(hdfsFile));
    }

}
