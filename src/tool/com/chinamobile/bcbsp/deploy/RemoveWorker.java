/**
 * CopyRight by Chinamobile
 * 
 * RemoveWorker.java
 */
package com.chinamobile.bcbsp.deploy;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;


public class RemoveWorker {
	
	String newUserName;
	String newIpAddress;
	String newHostName;
	String hadoopSourcePath;
	String hadoopDstPath;
	String bcbspSourcePath;
	String bcbspDstPath;
	
	public  RemoveWorker(String newUserName, String newIpAddress, String newHostName,
			String hadoopSourcePath, String hadoopDstPath, String bcbspSourcePath, String bcbspDstPath) {
		this.newUserName = newUserName;
		this.newIpAddress = newIpAddress;
		this.newHostName = newHostName;
		
		this.hadoopSourcePath = hadoopSourcePath;
		this.hadoopDstPath = hadoopDstPath;
		this.bcbspSourcePath = bcbspSourcePath;
		this.bcbspDstPath = bcbspDstPath;
	}
	
	public void closeDaemon() throws Exception {
		/*String command_stop_hdfs = ("ssh -l " + this.newUserName + " " + this.newIpAddress  + " " + "\"" 
				+ "$HADOOP_HOME/bin/hadoop-daemon.sh stop datenode" + "\"");*/
		String command_stop_bcbsp = ("ssh -l " + this.newUserName + " " + this.newIpAddress  + " " + "\"" 
				+ "$BCBSP_HOME/bin/bcbsp-daemon.sh stop workermanager" + "\"");
		//String cmd3[] = {"/bin/bash", "-c", command_stop_hdfs};
		String cmd4[] = {"/bin/bash", "-c", command_stop_bcbsp};
		//Process pro_hadoop = Runtime.getRuntime().exec(cmd3);
		Process pro_bcbsp  = Runtime.getRuntime().exec(cmd4);
		//pro_hadoop.waitFor();
		pro_bcbsp.waitFor();
	}
	
	public void changeWorkermanager(String rootPath, String hadoopIP, String hadoopUserName,
			String bcbspIP, String bcbspUserName) throws Exception {
		String command = null;
		String[] cmd = {"/bin/bash", "-c", command};
		Process p;
		
		// change Hadoop
		command = "scp " + hadoopUserName + "@" + hadoopIP + ":" + this.hadoopDstPath + "/" + Util.HadoopConf.HADOOP_CONF_DIR + "/"
				+ Util.HadoopConf.HADOOP_CONF_SLAVES_FILE + " " + rootPath + "/"
				+ Util.SystemConf.DEPLOY_TEMP_DIR + "/";
		cmd[2] = command;
		p = Runtime.getRuntime().exec(cmd);
		p.waitFor();
		
		File hadoopFile = new File(rootPath + "/" + Util.SystemConf.DEPLOY_TEMP_DIR, Util.HadoopConf.HADOOP_CONF_SLAVES_FILE);
		File hadoopFileRead = new File(rootPath + "/" + Util.SystemConf.DEPLOY_TEMP_DIR, Util.HadoopConf.HADOOP_CONF_SLAVES_FILE + ".tmp");
		File hadoopFileWrite = new File(rootPath + "/" + Util.SystemConf.DEPLOY_TEMP_DIR, Util.HadoopConf.HADOOP_CONF_SLAVES_FILE);
		hadoopFile.renameTo(hadoopFileRead);
		
		FileReader hadoopFR = new FileReader(hadoopFileRead);
		BufferedReader hadoopBR = new BufferedReader(hadoopFR);
		FileWriter hadoopFW = new FileWriter(hadoopFileWrite);
		BufferedWriter hadoopBW = new BufferedWriter(hadoopFW);
		
		String read = null;
		while ((read = hadoopBR.readLine()) != null) {
			if (read.equals(this.newHostName)) {
				continue;
			}
			hadoopBW.write(read);
			hadoopBW.newLine();
		}
		
		hadoopBR.close();
		hadoopFR.close();
		hadoopBW.close();
		hadoopFW.close();
		
		command = "scp " + hadoopFile.toString() + " " + hadoopUserName
				+ "@" + hadoopIP + ":" + this.hadoopDstPath + "/" + Util.HadoopConf.HADOOP_CONF_DIR + "/";
		cmd[2] = command;
		p = Runtime.getRuntime().exec(cmd);
		p.waitFor();
		hadoopFileRead.delete();
		hadoopFileWrite.delete();
		
		// change BCBSP
		command = "scp " + bcbspUserName + "@" + bcbspIP + ":" + this.bcbspDstPath + "/" + Util.BCBSPConf.BCBSP_CONF_DIR + "/"
			+ Util.BCBSPConf.BCBSP_CONF_WORKERS_FILE+ " " + rootPath + "/"
			+ Util.SystemConf.DEPLOY_TEMP_DIR + "/";
		cmd[2] = command;
		p = Runtime.getRuntime().exec(cmd);
		p.waitFor();

		File bcbspFile = new File(rootPath + "/" + Util.SystemConf.DEPLOY_TEMP_DIR, Util.BCBSPConf.BCBSP_CONF_WORKERS_FILE);
		File bcbspFileRead = new File(rootPath + "/" + Util.SystemConf.DEPLOY_TEMP_DIR, Util.BCBSPConf.BCBSP_CONF_WORKERS_FILE + ".tmp");
		File bcbspFileWrite = new File(rootPath + "/" + Util.SystemConf.DEPLOY_TEMP_DIR, Util.BCBSPConf.BCBSP_CONF_WORKERS_FILE);
		bcbspFile.renameTo(bcbspFileRead);
		
		FileReader bcbspFR = new FileReader(bcbspFileRead);
		BufferedReader bcbspBR = new BufferedReader(bcbspFR);
		FileWriter bcbspFW = new FileWriter(bcbspFileWrite);
		BufferedWriter bcbspBW = new BufferedWriter(bcbspFW);
		
		read = null;
		while ((read = bcbspBR.readLine()) != null) {
			if (read.equals(this.newHostName)) {
				continue;
			}
			bcbspBW.write(read);
			bcbspBW.newLine();
		}
		
		bcbspBR.close();
		bcbspFR.close();
		bcbspBW.close();
		bcbspFW.close();

		command = "scp " + bcbspFile.toString() + " " + bcbspUserName
				+ "@" + bcbspIP + ":" + this.bcbspDstPath + "/" + Util.BCBSPConf.BCBSP_CONF_DIR + "/";
		cmd[2] = command;
		p = Runtime.getRuntime().exec(cmd);
		p.waitFor();
		bcbspFileRead.delete();
		bcbspFileWrite.delete();
	}
}
