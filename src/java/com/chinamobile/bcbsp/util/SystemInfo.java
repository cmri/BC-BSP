/**
 * SystemInfo.java
 */
package com.chinamobile.bcbsp.util;

/**
 * SystemInfor
 * 
 * A version information class.
 * 
 * @author
 * @version
 */
public class SystemInfo {

    public static String getVersionInfo() {
        return "release-1.0";
    }
    
    public static String getSourceCodeInfo() {
        return "http://221.183.16.9/svn/bcbsp";
    }
    
    public static String getCompilerInfo() {
        return "China Mobile" 
                + " on " + getCompilerDateInfo();
    }
    
    private static String getCompilerDateInfo() {
        return "2012-12-04";
    }
}