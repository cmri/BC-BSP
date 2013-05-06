/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.partition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import junit.framework.TestCase;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.partition.HashPartitioner;
import com.chinamobile.bcbsp.partition.RecordParseDefault;


public class PartitionTest extends TestCase {
    int numPartition = 3;

    @Test
    public void testPartition() throws IOException, InterruptedException {
        HashPartitioner<Text> hashPartition = new HashPartitioner<Text>();
        hashPartition.setNumPartition(numPartition);
        File file = new File("/root/workspace3.3.2/sssp01");
        ///root/workspace3.3.2
        //./src/test/com/chinamobile/bcbsp/test/partition/datain
        BufferedReader reader = null;
        try {
            // System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行
            while (((tempString = reader.readLine()) != null)
                    && (tempString.length() > 0)) {
                // 显示行
                System.out.println("line " + line + ": " + tempString);

                String[] kvStrings = tempString.split(Constants.KV_SPLIT_FLAG);
                Text key;
                Text value;
                if (kvStrings.length != 2) {
                    key = new Text(kvStrings[0]);
                    value = null;
                } else {
                    key = new Text(kvStrings[0]);
                    value = new Text(kvStrings[1]);
                }

                System.out.println("line " + line + ": " + "key  "
                        + key.toString());
                if (value == null) {
                    System.out
                            .println("line " + line + ": " + "value  " + "have no values");
                } else {
                    System.out.println("line " + line + ": " + "value  "
                            + value.toString());
                }

                RecordParseDefault recordParse = new RecordParseDefault();
                String[] vertexArray = kvStrings[0].split(Constants.SPLIT_FLAG);
                Text expectedID = new Text(vertexArray[0]);
                Text vertexID = recordParse.getVertexID(key);

                assertEquals(expectedID, vertexID);

                System.out.println("line " + line + ": " + "vertexID  "
                        + vertexID);
                line++;
                int pid = -1;
                if (vertexID != null) {
                    pid = hashPartition.getPartitionID(vertexID);
                }
                System.out.println("PartitionID  " + pid);
                long expectedpid = -1;
                expectedpid = testID(vertexID);

                assertEquals(expectedpid, pid);
            }

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }

    public long testID(Text vertexID) {

        String id = vertexID.toString();
        MessageDigest md5 = null;
        if (md5 == null) {
            try {
                md5 = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {

                throw new IllegalStateException("no md5 algorythm found");
            }
        }
        md5.reset();
        md5.update(id.getBytes());
        byte[] bKey = md5.digest();
        long hashcode = (( long ) (bKey[3] & 0xFF) << 24)
                | (( long ) (bKey[2] & 0xFF) << 16)
                | (( long ) (bKey[1] & 0xFF) << 8) | ( long ) (bKey[0] & 0xFF);
        int result = ( int ) (hashcode % numPartition);
        result = (result < 0 ? result + numPartition : result);
        return result;
    }
}
