/**
 * CopyRight by Chinamobile
 * 
 * Util.java
 */
package com.chinamobile.bcbsp.deploy;
import java.io.BufferedWriter;


public class Util {
	
	public static class SystemConf {
		public static String MASTER_NAME_HEADER = "three-master-name";
		public static String SYSTEM_CHECK_FILE = "profile";
		public static String JDK_HOME_CHECK_HEADER = "JAVA_HOME=";
		public static String BCBSP_HOME_PATH_HEADER = "BCBSP_HOME=";
		public static String HADOOP_HOME_PATH_HEADER = "HADOOP_HOME=";
		
		public static String DEPLOY_CACHE_DIR = "cache";
		public static String DEPLOY_CACHE_File = "cache.info";
		
		public static String DEPLOY_TEMP_DIR = "tmp";
	}
	
	public static class BCBSPConf {
		public static String BCBSP_CONF_DIR = "conf";
		public static String BCBSP_CONF_ENV_FILE = "bcbsp-env.sh";
		public static String BCBSP_CONF_SITE_FILE = "bcbsp-site.xml";
		public static String BCBSP_CONF_WORKERS_FILE = "workermanager";
	}
	
	public static class HadoopConf {
		public static String HADOOP_CONF_DIR = "conf";
		public static String HADOOP_CONF_ENV_FILE = "hadoop-env.sh";
		public static String HADOOP_CONF_CORE_FILE = "core-site.xml";
		public static String HADOOP_CONF_HDFS_FILE = "hdfs-site.xml";
		public static String HADOOP_CONF_MAPRED_FILE = "mapred-site.xml";
		public static String HADOOP_CONF_SLAVES_FILE = "slaves";
	}
	
	public static class XML {
		public static String VERSION_INFO = "<?xml version=\"1.0\"?>";
		public static String TYPE_INFO = "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>";
		public static String CONTENT_START = "<configuration>";
		public static String CONTENT_END = "</configuration>";
		public static String PROPERTY_START = "<property>";
		public static String PROPERTY_END = "</property>";
		public static String PROPERTY_NAME_START = "<name>";
		public static String PROPERTY_NAME_END = "</name>";
		public static String PROPERTY_VALUE_START = "<value>";
		public static String PROPERTY_VALUE_END = "</value>";
		
		public static String filter(String content, String startFlag, String endFlag) {
			String result = null;
			int start = -1, end = -1;
			start = content.indexOf(startFlag);
			end = content.indexOf(endFlag);
			
			if (start != -1 && end != -1) {
				result = content.substring(start + startFlag.length(), end);
			}
			
			return result;
		}
		
		public static void writeHeader(BufferedWriter bw) throws Exception {
			bw.write(VERSION_INFO); bw.newLine();
			bw.write(TYPE_INFO); bw.newLine();
			bw.write(CONTENT_START);
		}
		
		public static void writeEnd(BufferedWriter bw) throws Exception {
			bw.write(CONTENT_END);
		}
		
		public static void writeRecord(BufferedWriter bw, String name, String value) throws Exception {
			bw.newLine();
			bw.write(PROPERTY_START); bw.newLine();
			bw.write(PROPERTY_NAME_START + name + PROPERTY_NAME_END); bw.newLine();
			bw.write(PROPERTY_VALUE_START + value + PROPERTY_VALUE_END); bw.newLine();
			bw.write(PROPERTY_END);
		}
	}
}