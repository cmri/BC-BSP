/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.fault.tools;

import java.io.File;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;
import com.chinamobile.bcbsp.fault.tools.Zip;

public class ZipTest extends TestCase {

    File file;
    String fileName;
    String path;

    @Before
    public void setUp() throws Exception {

        path = "src/test/com/chinamobile/bcbsp/test/fault/tools/ToCompress.txt";
        file = new File(path);
        fileName = path;
    }

    @Test
    public void testCompressFile() {
        String filePath = file.getAbsolutePath();
        char ch = '.';
        String expected = filePath.substring(0, filePath
                .lastIndexOf(( int ) ch))
                + ".zip";
        assertEquals(expected, Zip.compress(file));
    }

    @Test
    public void testCompressString() {
        char ch = '.';
        String expected = path.substring(0, path.lastIndexOf(( int ) ch))
                + ".zip";
        assertEquals(expected, Zip.compress(fileName));
    }

}
