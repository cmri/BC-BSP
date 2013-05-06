/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;

public class BSPConfigurationSetter {
    private ClassLoader classloader = null;
    private File conffile = null;
    private BSPConfiguration conf = null;
    private File bcbsptestDir = null;

    private static Integer currentPort = 0;

    public File getBcbsptestDir() {
        return bcbsptestDir;
    }

    public void setConf(BSPConfiguration conf) {
        this.conf = conf;
    }

    public void setBSPControllerAddress(String address) {
        if (conf == null) {
            conf = new BSPConfiguration();
        }
        conf.set(Constants.BC_BSP_CONTROLLER_ADDRESS, address);
    }

    public void mkTestDir(String dir) {
        int index = 0;
        bcbsptestDir = new File(dir + index);
        while (bcbsptestDir.exists()) {
            index++;
            bcbsptestDir = new File(dir + index);
        }
        bcbsptestDir.mkdirs();
    }

    public File getClasspathDir() {
        if (classloader == null) {
            classloader = Thread.currentThread().getContextClassLoader();
        }
        String[] classpathes = System.getProperty("java.class.path").split(":");
        File classpath = null;
        for (int i = 0; i < classpathes.length; i++) {
            classpath = new File(classpathes[i]);
            if (classpath.exists() && classpath.isDirectory()
                    && classpath.canExecute()) {
                break;
            }
            classpath = null;
        }
        return classpath;
    }

    public void setUp() throws FileNotFoundException {
        if (classloader == null) {
            classloader = Thread.currentThread().getContextClassLoader();
        }
        if (classloader.getResource("bcbsp-site.xml") == null) {
            File classpath = getClasspathDir();
            if (classpath == null) {
                System.out.println("Please check your classpath");
                throw new FileNotFoundException("No class path");
            }

            System.out.println(classpath);
            conffile = new File(classpath, "bcbsp-site.xml");

            FileOutputStream fos = new FileOutputStream(conffile);
            try {
                conf.writeXml(fos);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void cleanUp() {
        if (conffile != null) {
            deleteRecursiveOnExist(conffile);
        }
        if (bcbsptestDir != null) {
            deleteRecursiveOnExist(bcbsptestDir);
        }
    }

    public static boolean deleteRecursive(File file) {
        File[] files = file.listFiles();
        if (files != null) {
            for (int i = 0; i < files.length; i++)
                deleteRecursive(files[i]);
        }
        return file.delete();
    }

    public static boolean deleteRecursiveOnExist(File file) {
        if (file.exists())
            return deleteRecursive(file);
        return false;
    }

    public static int getFreePort(int start) {
        ServerSocket ss;
        int port = start > 20000 ? start : 20000;
        synchronized (currentPort) {
            currentPort++;
            port = port > currentPort ? port : currentPort;
            for (; port < 65535; port++) {
                try {
                    ss = new ServerSocket(port);
                    ss.close();
                    currentPort = port;
                    return port;
                } catch (IOException e) {

                }
            }
        }
        return 60000;
    }
}
